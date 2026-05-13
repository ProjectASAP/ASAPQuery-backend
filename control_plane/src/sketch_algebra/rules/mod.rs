//! Layer 4 `Bind*` rules — transform L3 [`QueryExpr`] sub-trees into L4
//! [`PhysicalExpr`] sub-trees.
//!
//! Per `control_plane/docs/design.md` §6 ("`core::optimizer` — Layer 4
//! framework", around line ~689) and §6 sketch_algebra (line ~565). A
//! `Bind*` rule:
//!
//! 1. Pattern-matches on a `QueryExpr::Aggregate` shape.
//! 2. Reads the [`AccuracyTarget`] off the matched intent.
//! 3. Consults the catalog (here: the family-default capability flags in
//!    [`crate::sketch_algebra::schema::SketchStateSchema::for_kind`]).
//! 4. Returns `Some(PhysicalExpr)` if it can bind, `None` otherwise.
//!
//! Rule selection is cost-aware: when multiple rules match (e.g. KLL vs
//! DDSketch on a `Quantile` intent), the dispatcher picks one by
//! consulting per-rule [`Rule::priority`] + the accuracy-driven hints
//! returned by [`Rule::cost_hint`]. The `cost_hint` is intentionally
//! coarse for Phase C — the cost-model integration is Phase F's domain.
//!
//! Each rule documents the binding with a comment referencing
//! `accuracy_profile.rs` (in ASAPQuery-backend) for the formal accuracy
//! bound that justifies the chosen parameter mapping.

#![allow(dead_code)]

pub mod bind_archive_only;
pub mod bind_cms_count;
pub mod bind_cms_topk;
pub mod bind_ddsketch_quantile;
pub mod bind_hll_cardinality;
pub mod bind_kll_quantile;

use crate::intent_algebra::QueryExpr;
use crate::sketch_algebra::physical_expr::PhysicalExpr;
use crate::types_v2::AccuracyTarget;

/// Bind-rule trait. Phase C keeps the trait minimal — `apply` + a
/// `priority` for the dispatcher's tie-break + a `cost_hint` (intent +
/// accuracy → relative cost). Phase F wires `cost_hint` into the
/// per-deployment cost model.
pub trait Rule: Sync {
    /// Stable, human-readable identifier (`"bind_kll_quantile"`, …) —
    /// used for diagnostics + the `cargo test` matrix.
    fn name(&self) -> &'static str;

    /// Lower the matched sub-tree under the given accuracy target. Return
    /// `None` if this rule does not apply to the supplied `expr`.
    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr>;

    /// Coarse priority used for tie-break when multiple rules match.
    /// Higher = preferred. Default 0.
    fn priority(&self) -> u16 {
        0
    }
}

/// Dispatch a `QueryExpr` sub-tree against the full Phase C rule set.
/// Returns the highest-priority binding that fires, or `None` if no rule
/// matches (caller wraps the input in `PhysicalExpr::Logical`).
pub fn dispatch(expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr> {
    let rules: Vec<Box<dyn Rule>> = vec![
        Box::new(bind_kll_quantile::BindKllOnQuantile),
        Box::new(bind_ddsketch_quantile::BindDDSketchOnQuantile),
        Box::new(bind_cms_count::BindCmsOnCount),
        Box::new(bind_cms_topk::BindCountSketchOnTopK),
        Box::new(bind_hll_cardinality::BindHllOnCardinality),
        // Phase β: archive-only catch-all. Lowest priority — fires only
        // when no warm-tier rule matches AND the intent is archive-only.
        Box::new(bind_archive_only::BindArchiveOnly),
    ];

    let mut best: Option<(u16, PhysicalExpr)> = None;
    for r in &rules {
        if let Some(out) = r.apply(expr, accuracy) {
            let p = r.priority();
            best = match best {
                Some((bp, _)) if bp >= p => best,
                _ => Some((p, out)),
            };
        }
    }
    best.map(|(_, e)| e)
}
