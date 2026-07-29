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
use std::rc::Rc;

use asap_sketch::{L4Node, SummaryExpr};

use crate::physical::colored_dag::dag::{ColoredDag, ColoredNode, NodeId};
use crate::physical::colored_dag::stage_id::{StageId, Topology};
use crate::sketch_algebra::physical_expr::L4Plan;
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
        match expr {
            PhysicalExpr::Committed(plan) => self.visit_plan(plan),

            // ── Phase ε.1 Mode 2: raw at edge, sketch built at backend.
            // Edge ships raw OTLP — we stage as Edge so the L5 emitter's
            // edge-side YAML pipeline picks it up; the sketch construction
            // itself happens at the backend (no edge sketch processor).
            PhysicalExpr::RawAtEdgeSketchAtBackend { child, .. } => {
                let id = self.reserve_node(expr.clone());
                let (cid, _) = self.visit_plan(child)?;
                self.dag.edges.push((id, cid));
                self.finish_node(id, StageId::Edge)
            }

            // ── Phase ε.1 Mode 3: raw at edge, ships directly to
            // Prometheus's native OTLP receiver. The agent pipeline picks
            // this up via `asap.mode=prometheus_archive` routing.
            PhysicalExpr::RawAtEdgePrometheusArchive { .. } => {
                let id = self.reserve_node(expr.clone());
                self.finish_node(id, StageId::Edge)
            }
        }
    }

    /// Recursively visit an [`L4Plan`] — the "what to compute" layer.
    /// [`L4Plan::Summary`] delegates the actual per-node granularity to
    /// [`Self::visit_l4node`] (walking `asap_sketch::L4Node`'s own DAG
    /// shape); [`L4Plan::LetBinding`] / [`L4Plan::Ref`] are this crate's
    /// own named-binding sharing mechanism, unchanged from before Step B.
    fn visit_plan(&mut self, plan: &L4Plan) -> Result<(NodeId, StageId), AllocateError> {
        match plan {
            L4Plan::Summary(node) => self.visit_l4node(node),

            // ── LetBinding: colour by the bound expression's stage,
            // and bring the binding into scope before walking the body.
            L4Plan::LetBinding { name, expr, child } => {
                let id = self.reserve_node(PhysicalExpr::Committed(plan.clone()));
                let (eid, expr_stage) = self.visit_plan(expr)?;
                self.dag.edges.push((id, eid));
                self.scope.insert(name.as_str().to_string(), expr_stage);
                let (bid, _) = self.visit_plan(child)?;
                self.dag.edges.push((id, bid));
                self.finish_node(id, expr_stage)
            }

            // ── Ref: colour matches the binding's stage. Unresolved
            // refs bubble up as `AllocateError::UnresolvedRef`.
            L4Plan::Ref { name } => {
                let id = self.reserve_node(PhysicalExpr::Committed(plan.clone()));
                let stage = self
                    .scope
                    .get(name.as_str())
                    .copied()
                    .ok_or_else(|| AllocateError::UnresolvedRef(name.as_str().to_string()))?;
                self.finish_node(id, stage)
            }
        }
    }

    /// Recursively visit one `asap_sketch::L4Node` — the sketch algebra
    /// itself, owned upstream. Every semantic node gets its own
    /// [`ColoredNode`] (matching the granularity the old, locally-defined
    /// `PhysicalExpr::{SketchAgg,SketchEstimate,SketchMerge}` had),
    /// stored back as `PhysicalExpr::Committed(L4Plan::Summary(..))`
    /// wrapping just that sub-node, so downstream consumers
    /// (`colored_dag::emitter`, `emit::mod`) keep pattern-matching
    /// against the same `PhysicalExpr` shape.
    fn visit_l4node(&mut self, node: &Rc<L4Node>) -> Result<(NodeId, StageId), AllocateError> {
        let id = self.reserve_node(PhysicalExpr::committed(Rc::clone(node)));

        let stage = match &node.expr {
            // ── Logical pass-through — colour by inspecting the wrapped
            // L3 QueryExpr. `Scan` / `Window` always land on edge;
            // `Aggregate{exact}` lands on edge if its child is an edge
            // (scrape locality); `Ref` resolves through the lexical
            // scope map.
            SummaryExpr::Logical(qe) => self.colour_logical(qe)?,

            // ── SummaryAgg: always edge per design.md §6 batched-queries
            // table — true for both approximate sketches (the old
            // `SketchAgg`) and exact accumulators (the old `ExactAgg`);
            // `SummaryKind` unifies both into the same node shape, and
            // both landed on Edge before this migration too.
            SummaryExpr::SummaryAgg { child, .. } => {
                let (cid, _) = self.visit_l4node(child)?;
                self.dag.edges.push((id, cid));
                StageId::Edge
            }

            // ── SummaryEstimate: always backend per design.md §6.
            // The "SketchEstimate MUST be on the same stage as its
            // consumers (typically Backend)" invariant is satisfied
            // because consumers above SummaryEstimate are also backend.
            SummaryExpr::SummaryEstimate { summary_input, .. } => {
                let (cid, child_stage) = self.visit_l4node(summary_input)?;
                self.dag.edges.push((id, cid));
                // If child is on edge or gateway, this is a cross-stage
                // edge — that's expected (the wire-format hop).
                let _ = child_stage;
                StageId::Backend
            }

            // ── SummaryMerge: gateway under three-stage. Children are
            // edge SummaryAgg outputs.
            SummaryExpr::SummaryMerge { children } => {
                for child in children {
                    let (cid, _) = self.visit_l4node(child)?;
                    self.dag.edges.push((id, cid));
                }
                StageId::Gateway
            }

            // ── SummaryJoin / SummarySubtract / SummaryDelete: not
            // surfaced by any `Bind*` path yet (gated on rules that
            // haven't landed — see `physical_expr.rs`'s module docs'
            // predecessor note). Conservative default matching
            // SummaryMerge's multi-input-combination shape until a real
            // consumer picks a placement.
            SummaryExpr::SummaryJoin { outer, inner, .. } => {
                let (oid, _) = self.visit_l4node(outer)?;
                self.dag.edges.push((id, oid));
                let (iid, _) = self.visit_l4node(inner)?;
                self.dag.edges.push((id, iid));
                StageId::Gateway
            }
            SummaryExpr::SummarySubtract { left, right } => {
                let (lid, _) = self.visit_l4node(left)?;
                self.dag.edges.push((id, lid));
                let (rid, _) = self.visit_l4node(right)?;
                self.dag.edges.push((id, rid));
                StageId::Gateway
            }
            SummaryExpr::SummaryDelete { summary_input, .. } => {
                let (cid, _) = self.visit_l4node(summary_input)?;
                self.dag.edges.push((id, cid));
                StageId::Gateway
            }
        };

        self.finish_node(id, stage)
    }

    /// Reserve a slot for a node up-front so child IDs are strictly
    /// larger than the parent's; downstream `cut_edges` analysis assumes
    /// parents come before children in `nodes`.
    fn reserve_node(&mut self, expr: PhysicalExpr) -> NodeId {
        let id = NodeId(self.dag.nodes.len());
        self.dag.nodes.push(ColoredNode {
            id,
            expr,
            // Placeholder — overwritten by `finish_node` once children
            // have been coloured.
            stage: StageId::Edge,
        });
        id
    }

    /// Patch in the resolved stage now that children have been visited.
    fn finish_node(
        &mut self,
        id: NodeId,
        stage: StageId,
    ) -> Result<(NodeId, StageId), AllocateError> {
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
            // rules for Filter/Project/Distinct/Sort/Limit/BinaryOp (and,
            // since the `asap_ir` merge, the PromQL-surface superset —
            // Scalar/EvalTime/VectorFromScalar/ScalarFromVector/Relabel/
            // InfoJoin/Sample/TimeRange/TimeShift/WindowFunc, also
            // unconstructed here today) land alongside their consumers in
            // follow-up batches. `Partition` no longer exists in the
            // canonical IR — its keys fold into `Aggregate.by` at
            // construction time (`intent_algebra::lower`).
            QE::Merge { .. } | QE::Join { .. } | QE::SetOp { .. } | QE::BinaryOp { .. } => {
                Ok(StageId::Backend)
            }
            // Filter/Project/Distinct/Sort/Limit/Subquery, plus the
            // PromQL-surface superset unconstructed here today, all fall
            // through to this Edge default.
            _ => Ok(StageId::Edge),
        }
    }
}

// Convenience helper used by tests / external callers that only need a
// stage lookup keyed by binding name.
pub(crate) fn binding_stage(dag: &ColoredDag, name: &BindingName) -> Option<StageId> {
    dag.nodes.iter().find_map(|n| match &n.expr {
        PhysicalExpr::Committed(L4Plan::LetBinding { name: n2, .. }) if n2 == name => {
            Some(n.stage)
        }
        _ => None,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::intent_algebra::{BindingScope, QueryExpr, Schema, Source, WindowKind};
    use std::time::Duration;

    fn ts_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "http_request_duration_seconds".into(),
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
        let leaf = PhysicalExpr::committed(
            asap_plan::bind::logical(&ts_scan(), &BindingScope::default()).unwrap(),
        );
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
        let q = QueryExpr::Aggregate {
            reduction: crate::intent_algebra::Reduction::PerEntity,
            aggs: vec![crate::intent_algebra::AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let node = asap_plan::bind::implement_tree(&q).unwrap();
        let expr = PhysicalExpr::committed(node);
        let dag = StageAllocator
            .allocate(&expr, Topology::ThreeStage)
            .unwrap();
        // root = SummaryEstimate → Backend
        assert_eq!(dag.root().unwrap().stage, StageId::Backend);
        // node 1 = SummaryAgg → Edge
        assert_eq!(dag.nodes[1].stage, StageId::Edge);
        // node 2 = Logical(Window) → Edge
        assert_eq!(dag.nodes[2].stage, StageId::Edge);
    }
}
