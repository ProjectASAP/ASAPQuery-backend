//! Backend-facing emission for a compiled physical plan.
//!
//! * [`backend_wire`] builds the storage-routing table and the aggregation /
//!   readout JSON the backend's `PrecomputeMaterialization` parser consumes.
//! * [`monitor`] carries the CDM monitor declarations.

pub mod backend_wire;
pub mod monitor;

use crate::physical::post_asap::deployment_expr::PostAsapPlan;
use crate::physical::post_asap::PhysicalExpr;
use planner_types::post_asap::{SketchAlgorithm, SummaryExpr, SummaryNode};
use std::rc::Rc;

/// Walk a `PhysicalExpr` tree and return the first `SketchAgg::sketch_type`
/// (or the `RawAtEdgeSketchAtBackend::family` Mode-2 equivalent). The
/// canonical shape produced by `bind_workload_typed` is
/// `SketchEstimate { child: SketchAgg { sketch_type, … } }`, so this is
/// effectively a one-level descent — but we walk recursively to stay
/// robust against future shape changes (e.g. Bind* rules wrapping
/// in `LetBinding` for fan-in shared sketches).
///
/// Returns `None` only for trees that carry no sketch commitment
/// (`Logical`-only, unresolved `Ref`, raw Mode-3 archive). These map
/// onto the raw-passthrough default pipeline in the routing emitter,
/// which is correct.
pub fn extract_root_sketch_algorithm(expr: &PhysicalExpr) -> Option<SketchAlgorithm> {
    match expr {
        PhysicalExpr::Committed(plan) => extract_from_plan(plan),
        PhysicalExpr::RawAtEdgeSketchAtBackend { family, .. } => Some(family.clone()),
        PhysicalExpr::RawAtEdgePrometheusArchive { .. } => None,
    }
}

fn extract_from_plan(plan: &PostAsapPlan) -> Option<SketchAlgorithm> {
    match plan {
        PostAsapPlan::Summary(node) => extract_from_node(node),
        PostAsapPlan::LetBinding { expr, child, .. } => {
            extract_from_plan(expr).or_else(|| extract_from_plan(child))
        }
        PostAsapPlan::Ref { .. } => None,
    }
}

fn extract_from_node(node: &Rc<SummaryNode>) -> Option<SketchAlgorithm> {
    match &node.expr {
        // The `family` variant distinguishes exact accumulators from sketches.
        SummaryExpr::SummaryAgg {
            family: planner_types::post_asap::SummaryFamilyType::Sketch(kind, _),
            ..
        } => Some(kind.algorithm().clone()),
        // An exact accumulator has no sketch family beneath it (its own
        // child is always a plain `Logical` leaf) — same as the old
        // `ExactAgg` case.
        SummaryExpr::SummaryAgg { .. } => None,
        SummaryExpr::SummaryEstimate { summary_input, .. } => extract_from_node(summary_input),
        SummaryExpr::SummaryMerge { children, .. } => children.iter().find_map(extract_from_node),
        SummaryExpr::ValueOperation { child, .. } => extract_from_node(child),

        // Not surfaced by any `Bind*` path yet (gated on rules that
        // haven't landed — see `deployment_expr.rs`'s module docs).
        SummaryExpr::BinaryOp { .. }
        | SummaryExpr::RelationalJoin { .. }
        | SummaryExpr::SummaryJoin { .. }
        | SummaryExpr::SummarySubtract { .. }
        | SummaryExpr::SummaryDelete { .. }
        | SummaryExpr::KeepPreAsap(_) => None,
    }
}
