//! Cost-based fixed-point query optimizer.
//!
//! The optimizer applies a set of algebraic rewrite rules to a canonical
//! [`QueryExpr`](planner_types::pre_asap::QueryExpr) tree until no rule fires
//! (fixed point).  Each rule is a pure function `QueryExpr → Option<QueryExpr>`:
//! returning `None` means "this rule does not apply here".
//!
//! # Rules implemented
//!
//! | Rule | Name | Description |
//! |------|------|-------------|
//! | R1 | `PredicatePushDown`     | Push `Filter` below `Window`, `Sort` |
//! | R2 | `MergeLifting`          | Lift a mergeable single-intent `Aggregate` above `Merge` |
//! | R3 | `HLLDedupElim`          | Eliminate `Distinct` before a cardinality `Aggregate` |
//! | R4 | `FilterWindowSwap`      | Swap `Filter` below `Window` to reduce window input size |
//! | R5 | `TopKFusion`            | Fuse `Limit(Sort DESC)` into a `TopK`-intent `Aggregate` |
//! | R6 | _(retired)_             | `HistogramQuantileFusion` retired in Step γ5 |
//! | R7 | _(retired)_             | `SubqueryDecorrelation` retired in the `asap_ir` merge — see `intent_algebra::lower` module docs |
//! | R8 | `CommonSubexprElim`     | Extract identical `Scan` sub-trees into `LetBinding`s |
//! | R9 | `HydraConversion`       | (disabled — Step γ TODO; see struct docs) |
//! | R10| `WindowMerge`           | Merge adjacent identical `Window` nodes |
//! | R11| _(retired)_             | `PartitionElim` retired — `Partition` no longer exists in the canonical IR; its keys fold into `Aggregate.by` at construction time |
//! | R12| `SetOpFusion`           | Fuse `SetOp(Union, Merge, Merge)` into a single `Merge` |
//!
//! Step γ7: the optimizer consumes and produces the canonical
//! `query_expr::QueryExpr`. The legacy `SketchAgg` / `WindowedAgg` / `TopK`
//! variants are gone — `SketchAgg` folds into a single-intent `Aggregate`,
//! `WindowedAgg` into `Window { Aggregate }`, and `TopK` into an
//! `Aggregate` carrying an `AggIntent::TopK`.

use std::rc::Rc;

use crate::optimizer::cost::sketch_capability::{
    default_capability_table, load_capability_overrides, SketchCapability,
};
use crate::types_v2::AccuracyTarget;
use planner_types::pre_asap::AggIntent;
use planner_types::pre_asap::{agg_is_exact, agg_is_mergeable};
use planner_types::pre_asap::{QueryExpr, Reduction, SetOpKind, Source};

// ── Cost model interface ──────────────────────────────────────────────────────

/// Estimated cost of evaluating an expression at a given bandwidth.
#[derive(Debug, Clone, Default)]
pub struct NodeCost {
    pub bytes_per_sec: f64,
    pub memory_bytes: f64,
    pub cpu_per_sample: f64,
}

/// Pluggable cost oracle.  The default implementation uses simple heuristics.
pub trait CostModel: Send + Sync {
    /// Estimate the cost of the expression tree rooted at `expr`.
    fn estimate(&self, expr: &QueryExpr) -> NodeCost;

    /// Deployment constraints (memory budgets, available backends, etc.).
    /// Returns `None` if no constraints are configured (unconstrained mode).
    fn constraints(&self) -> Option<&DeploymentConstraints> {
        None
    }
}

// ── Sketch capabilities ─────────────────────────────────────────────────────
//
// Per the Step 2a consolidation (later re-homed to `optimizer::cost` in
// Stage 4 of the `sketch_algebra` re-layering — this is a cost-model
// concern, not L4 IR), `SketchCapability` / `SupportedIntent` and the
// YAML-loader logic live in `crate::optimizer::cost::sketch_capability`.
// The optimizer re-exports `sketch_capability(SketchType)` and
// `load_sketch_capabilities(path)` as thin shims so existing callers
// (`algebra::physical`, `main.rs`) keep building while the legacy
// `crate::types::SketchType` key continues to be the lookup key.

/// Load sketch capabilities from a YAML file. Thin shim — the real
/// loader lives in `optimizer::cost::sketch_capability::load_capability_overrides`
/// and is keyed by `SketchAlgorithm`. This shim translates the result to the
/// legacy `SketchType` key used by call sites that haven't migrated.
///
/// Falls back to built-in defaults if the file is missing or malformed.
pub fn load_sketch_capabilities(
    path: &str,
) -> std::collections::HashMap<crate::types::SketchType, SketchCapability> {
    use crate::types::SketchType;
    let by_kind = load_capability_overrides(path);
    let mut out = std::collections::HashMap::new();
    for (k, v) in by_kind {
        out.insert(SketchType::from(k), v);
    }
    out
}

/// Built-in capability profile for a known sketch type. Thin shim —
/// the real defaults live in `optimizer::cost::sketch_capability::default_capability_table`.
pub fn sketch_capability(st: &crate::types::SketchType) -> SketchCapability {
    use planner_types::post_asap::SketchAlgorithm;
    let kind: SketchAlgorithm = st.clone().into();
    default_capability_table()
        .remove(&kind)
        .expect("default_capability_table covers every SketchAlgorithm variant")
}

// ── Stage budgets ───────────────────────────────────────────────────────────

/// Resource budget for a single pipeline stage.
///
/// All fields are `Option` — `None` means unbounded / unconstrained.
#[derive(Debug, Clone, Default)]
pub struct StageBudget {
    /// Memory budget (bytes).
    pub memory_bytes: Option<u64>,
    /// CPU budget (µs per sample).
    pub cpu_micros_per_sample: Option<f64>,
    /// Disk budget (bytes).
    pub disk_bytes: Option<u64>,
    /// Egress bandwidth budget (bytes/sec).
    pub bandwidth_bytes_per_sec: Option<f64>,
}

impl StageBudget {
    /// Check whether a sketch fits within this stage's budget.
    pub fn fits(&self, cap: &SketchCapability) -> bool {
        if let Some(mem) = self.memory_bytes {
            if cap.memory_bytes_per_series > mem {
                return false;
            }
        }
        if let Some(cpu) = self.cpu_micros_per_sample {
            if cap.cpu_micros_per_insert > cpu {
                return false;
            }
        }
        if let Some(bw) = self.bandwidth_bytes_per_sec {
            // Rough: transmission bytes per flush ÷ 1 second
            if cap.transmission_bytes as f64 > bw {
                return false;
            }
        }
        true
    }
}

// ── Deployment constraints ──────────────────────────────────────────────────

/// Full deployment specification: per-stage budgets.
///
/// The optimizer uses stage budgets to penalise plans that exceed capacity.
/// The physical planner uses `StageBudget::fits(SketchCapability)` to decide
/// concrete placement.
#[derive(Debug, Clone, Default)]
pub struct DeploymentConstraints {
    /// Edge / agent collector (sketch build).
    pub agent: StageBudget,
    /// Backend collector (sketch merge).
    pub backend_collector: StageBudget,
    /// Backend sketchDB (precompute engine / query engine).
    pub backend_db: StageBudget,
    /// Backend original DB (exact computation).
    pub original_db: StageBudget,
    /// Object store (S3 raw backup).
    pub object_store: StageBudget,
}

impl DeploymentConstraints {
    /// Build from [`StageResourceBudgets`] (the workload-derived budgets).
    pub fn from_budgets(budgets: &crate::types::StageResourceBudgets) -> Self {
        Self {
            agent: StageBudget {
                memory_bytes: budgets.agent_memory_bytes,
                cpu_micros_per_sample: budgets.agent_cpu_micros_per_sample,
                ..Default::default()
            },
            backend_collector: StageBudget {
                memory_bytes: budgets.backend_memory_bytes,
                ..Default::default()
            },
            backend_db: StageBudget {
                memory_bytes: budgets.precompute_memory_bytes,
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

/// Default cost model — simple heuristics, no schema statistics.
pub struct DefaultCostModel {
    pub raw_bytes_per_sec: f64,
    pub deployment: Option<DeploymentConstraints>,
}

/// The single aggregation intent of a single-intent, no-HAVING `Aggregate`
/// — the canonical fold of the legacy `SketchAgg`. `None` for any other
/// shape (multi-intent / HAVING aggregate, or a non-`Aggregate` node).
fn single_sketch_intent(expr: &QueryExpr) -> Option<&AggIntent> {
    match expr {
        QueryExpr::Aggregate {
            measures: aggs,
            having,
            ..
        } if aggs.len() == 1 && having.is_none() => Some(&aggs[0]),
        _ => None,
    }
}

impl CostModel for DefaultCostModel {
    fn constraints(&self) -> Option<&DeploymentConstraints> {
        self.deployment.as_ref()
    }

    fn estimate(&self, expr: &QueryExpr) -> NodeCost {
        // Sketch nodes reduce bandwidth; exact nodes pass through. The
        // canonical IR folds the legacy `SketchAgg` / `WindowedAgg` / `TopK`
        // into `Aggregate`, so the intent-driven factor reads the single
        // intent of a single-intent `Aggregate`.
        let factor = match expr {
            QueryExpr::Aggregate {
                measures: aggs,
                having,
                ..
            } if aggs.len() == 1 && having.is_none() => match &aggs[0] {
                AggIntent::Quantile { .. } => 0.05,
                AggIntent::Cardinality { .. } => 0.02,
                op if crate::planner_selection::as_frequency(op).is_some() => 0.03,
                AggIntent::TopK { k, .. } => (*k as f64).recip().min(0.1),
                op if agg_is_exact(op) => 1.0,
                _ => 0.1,
            },
            QueryExpr::Concat { children } => 1.0 / (children.len().max(1) as f64),
            QueryExpr::Filter { .. } => 0.5,
            QueryExpr::Dedup { .. } => 0.9,
            _ => 1.0,
        };

        // Single sketch intent (non-TopK) → catalog memory estimate;
        // everything else → bandwidth-scaled heuristic.
        let memory = match single_sketch_intent(expr) {
            Some(op) if !matches!(op, AggIntent::TopK { .. }) => {
                crate::physical::sketch_catalog::estimated_sketch_memory_bytes(op) as f64
            }
            _ => self.raw_bytes_per_sec * factor * 0.01,
        };

        let base = NodeCost {
            bytes_per_sec: self.raw_bytes_per_sec * factor,
            memory_bytes: memory,
            cpu_per_sample: factor * 10.0,
        };

        // Apply deployment constraint penalties.
        if let Some(dc) = &self.deployment {
            // Map each expression to its default stage budget.
            let stage_budget = match expr {
                QueryExpr::Scan { .. } | QueryExpr::Filter { .. } | QueryExpr::TimeRange { .. } => {
                    &dc.agent
                }
                QueryExpr::Aggregate {
                    measures: aggs,
                    having,
                    ..
                } if aggs.len() == 1 && having.is_none() => {
                    match &aggs[0] {
                        // TopK-intent aggregate runs at the precompute engine.
                        AggIntent::TopK { .. } => &dc.backend_db,
                        // Single sketch intent — the SketchAgg fold — at agent.
                        _ => &dc.agent,
                    }
                }
                // Multi-intent / HAVING aggregate → exact original DB.
                QueryExpr::Aggregate { .. } => &dc.original_db,
                QueryExpr::Concat { .. } | QueryExpr::Dedup { .. } => &dc.backend_collector,
                QueryExpr::BinaryOp { .. } | QueryExpr::PromqlSubquery { .. } => &dc.backend_db,
                _ => &dc.agent,
            };

            // For single-sketch-intent nodes, check sketch capability against
            // the stage budget.
            if let Some(op) = single_sketch_intent(expr) {
                if !matches!(op, AggIntent::TopK { .. }) {
                    let sketch_type = crate::physical::sketch_catalog::sketch_type_for_op(op);
                    let cap = sketch_capability(&sketch_type);
                    if !stage_budget.fits(&cap) {
                        // Sketch doesn't fit — apply 10× penalty across all dimensions.
                        return NodeCost {
                            bytes_per_sec: base.bytes_per_sec * 10.0,
                            memory_bytes: base.memory_bytes * 10.0,
                            cpu_per_sample: base.cpu_per_sample * 10.0,
                        };
                    }
                }
            } else {
                // Non-sketch nodes: check basic budget constraints.
                if let Some(budget) = stage_budget.memory_bytes {
                    if base.memory_bytes > budget as f64 {
                        return NodeCost {
                            memory_bytes: base.memory_bytes * 10.0,
                            ..base
                        };
                    }
                }
                if let Some(bw) = stage_budget.bandwidth_bytes_per_sec {
                    if base.bytes_per_sec > bw {
                        return NodeCost {
                            bytes_per_sec: base.bytes_per_sec * 10.0,
                            ..base
                        };
                    }
                }
            }
        }
        base
    }
}

// ── Rewrite rule trait ────────────────────────────────────────────────────────

/// A single algebraic rewrite rule.
///
/// Step γ7: the canonical `Aggregate.by` is already positional, so — unlike
/// the legacy `RewriteRule` — no inherited `Schema` is threaded; every rule
/// is a purely structural rewrite over the canonical IR.
pub trait RewriteRule: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Try to rewrite `expr`.  Returns `Some(new_expr)` if the rule fired,
    /// `None` otherwise.  The rule is applied top-down: the optimizer will
    /// also recurse into the children of `new_expr`.
    fn try_rewrite(&self, expr: QueryExpr, model: &dyn CostModel) -> Option<QueryExpr>;
}

// ── R1: PredicatePushDown ─────────────────────────────────────────────────────

/// Push `Filter` nodes as deep as possible — reduces data volume early.
///
/// * `Filter(p, Window(…, e))`    → `Window(…, Filter(p, e))`
/// * `Filter(p, Sort(k, e))`      → `Sort(k, Filter(p, e))`
///
/// No `Partition` case: folded into `Aggregate.by` at construction time
/// (`intent_algebra::lower`), so `Filter` never wraps a bare grouping
/// marker anymore — pushing a `Filter` below an `Aggregate` would change
/// which rows get aggregated, so that's not a safe pushdown.
pub struct PredicatePushDown;

impl RewriteRule for PredicatePushDown {
    fn name(&self) -> &'static str {
        "PredicatePushDown"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Filter { pred, child } => match (*child).clone() {
                // Filter below TimeRange (the canonical range-vector-selector
                // node -- see control_plane/docs/design-asapplanner-pin-migration.md).
                QueryExpr::TimeRange {
                    range,
                    child: inner,
                } => Some(QueryExpr::TimeRange {
                    range,
                    child: Rc::new(QueryExpr::Filter { pred, child: inner }),
                }),
                // Filter below Sort (safe when pred references input columns only)
                QueryExpr::Sort {
                    keys,
                    partition_by,
                    child: inner,
                } => Some(QueryExpr::Sort {
                    keys,
                    partition_by,
                    child: Rc::new(QueryExpr::Filter { pred, child: inner }),
                }),
                // Not applicable — reconstruct
                other => Some(QueryExpr::Filter {
                    pred,
                    child: Rc::new(other),
                }),
            },
            _ => None,
        }
    }
}

// ── R2: MergeLifting ──────────────────────────────────────────────────────────

/// Lift a mergeable single-intent `Aggregate` above a `Merge` node.
///
/// `Aggregate([op], Merge([a, b]))` → `Merge([Aggregate([op], a), Aggregate([op], b)])`
///
/// Only applied for `agg_is_mergeable` ops so we don't incorrectly
/// distribute `Avg`.
pub struct MergeLifting;

impl RewriteRule for MergeLifting {
    fn name(&self) -> &'static str {
        "MergeLifting"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Aggregate {
                ref reduction,
                measures: ref aggs,
                ref having,
                ref child,
                ..
            } if aggs.len() == 1 && having.is_none() && agg_is_mergeable(&aggs[0]) => {
                if let QueryExpr::Concat { children } = child.as_ref() {
                    let new_children: Vec<QueryExpr> = children
                        .iter()
                        .map(|branch| QueryExpr::Aggregate {
                            reduction: reduction.clone(),
                            measures: aggs.clone(),
                            output_names: Vec::new(),
                            having: None,
                            child: Rc::new(branch.clone()),
                        })
                        .collect();
                    return Some(QueryExpr::Concat {
                        children: new_children,
                    });
                }
                None
            }
            _ => None,
        }
    }
}

// ── R3: HLLDedupElim ─────────────────────────────────────────────────────────

/// Eliminate a `Distinct` node that immediately precedes a cardinality
/// (HLL) aggregation — HLL counts distinct values intrinsically.
///
/// `Aggregate([Cardinality], Distinct(e))` → `Aggregate([Cardinality], e)`
pub struct HLLDedupElim;

impl RewriteRule for HLLDedupElim {
    fn name(&self) -> &'static str {
        "HLLDedupElim"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Aggregate {
                reduction,
                measures: aggs,
                output_names,
                having: None,
                child,
            } if aggs.len() == 1 && matches!(&aggs[0], AggIntent::Cardinality { .. }) => {
                if let QueryExpr::Dedup { child: inner, .. } = child.as_ref() {
                    return Some(QueryExpr::Aggregate {
                        reduction,
                        measures: aggs,
                        output_names,
                        having: None,
                        child: inner.clone(),
                    });
                }
                None
            }
            _ => None,
        }
    }
}

// ── R4: FilterWindowSwap ──────────────────────────────────────────────────────

/// Push `Filter` below `Window`. Identical to the Window case in R1, kept
/// separate so the optimizer can attribute the transformation in logs.
pub struct FilterWindowSwap;

impl RewriteRule for FilterWindowSwap {
    fn name(&self) -> &'static str {
        "FilterWindowSwap"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Filter { pred, child } => {
                if let QueryExpr::TimeRange {
                    range,
                    child: inner,
                } = (*child).clone()
                {
                    return Some(QueryExpr::TimeRange {
                        range,
                        child: Rc::new(QueryExpr::Filter { pred, child: inner }),
                    });
                }
                None
            }
            _ => None,
        }
    }
}

// ── R5: TopKFusion ────────────────────────────────────────────────────────────

/// Fuse `Limit(n, Sort([… DESC], e))` into a single `Aggregate` carrying an
/// `AggIntent::TopK` — the canonical heavy-hitter shape the physical planner
/// maps to a `CountSketch`.
pub struct TopKFusion;

impl RewriteRule for TopKFusion {
    fn name(&self) -> &'static str {
        "TopKFusion"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Limit {
                n,
                offset: 0,
                child,
            } => {
                if let QueryExpr::Sort {
                    keys, child: inner, ..
                } = (*child).clone()
                {
                    // Only fuse when all keys are DESC (top-k semantics).
                    if !keys.is_empty() && keys.iter().all(|k| !k.ascending) {
                        return Some(QueryExpr::Aggregate {
                            reduction: Reduction::by(vec![]),
                            measures: vec![AggIntent::TopK {
                                k: n,
                                accuracy: AccuracyTarget::Epsilon(0.05),
                            }],
                            output_names: Vec::new(),
                            having: None,
                            child: inner,
                        });
                    }
                }
                None
            }
            _ => None,
        }
    }
}

// ── R6 (retired): HistogramQuantileFusion ─────────────────────────────────────
//
// Per Step γ5 of the relational migration, `histogram_quantile(φ, …)` is
// substituted at the PromQL parser level into a plain
// `Aggregate{Quantile(φ)}`. The fusion rule that used to merge a
// `HistogramQuantile` wrapper with an inner DDSketch is no longer needed.

// R7 (retired): `SubqueryDecorrelation` used to hoist a `ScalarSubquery`
// predicate into a `LetBinding`, pattern-matching the old `Predicate`
// enum's `BinaryOp` / `ScalarSubquery` / `Column(ColumnRef::Named(_))`
// variants directly. The canonical `Predicate(QueryExpr)` merge (see
// `intent_algebra::lower` module docs) rejects `ScalarSubquery` at
// construction time instead of lowering it, so this rule has no
// construction site left to fire against — deleted rather than ported.

// ── R8: CommonSubexprElim ─────────────────────────────────────────────────────

/// Identify identical `Scan` sub-trees that appear in multiple branches of
/// a `Merge` node and hoist them into a `LetBinding`.
///
/// **Disabled** (ASAPPlanner pin migration): the canonical `QueryExpr` has
/// no `Ref`/`LetBinding` variant anymore (ASAPPlanner#181/#192 deleted
/// them upstream as dead scaffolding; the canonical tree type genuinely
/// has no room for them — Rust doesn't allow adding variants to a foreign
/// enum). Unlike the workload-level `optimizer::cse` this repo deleted
/// outright (zero callers, confirmed dead), this rule is a real,
/// registered `RewriteRule` — but with the hoisting *representation*
/// gone, `try_rewrite` can no longer construct its result at all.
/// Always returns `None` (no rewrite) rather than invent a
/// representation upstream doesn't support; the query still computes the
/// correct answer without this optimization, just re-scanning a
/// duplicated metric across `Merge` branches instead of sharing one scan.
/// See control_plane/docs/design-asapplanner-pin-migration.md.
pub struct CommonSubexprElim;

/// The metric name of a `Scan { TimeSeries }` leaf, if `qe` is one.
fn scan_metric(qe: &QueryExpr) -> Option<&str> {
    match qe {
        QueryExpr::Scan {
            source: Source::TimeSeries { metric },
            ..
        } => Some(metric.as_str()),
        _ => None,
    }
}

impl RewriteRule for CommonSubexprElim {
    fn name(&self) -> &'static str {
        "CommonSubexprElim"
    }

    fn try_rewrite(&self, _expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        // See the struct doc: no `LetBinding`/`Ref` to hoist into anymore.
        None
    }
}

// ── R9: HydraConversion ───────────────────────────────────────────────────────

/// Convert multi-key `Partition + Aggregate` into a Hydra sketch-of-sketches.
///
/// Disabled since Step α — the legacy `AggIntent::PerPartition` variant is
/// gone and the canonical multi-key shape is `Aggregate { by: keys, .. }`.
/// Re-emitting that wrapper is a Step γ TODO. Disabling a cost-driven rule
/// preserves correctness (the unfused tree still produces the right result).
pub struct HydraConversion;

impl RewriteRule for HydraConversion {
    fn name(&self) -> &'static str {
        "HydraConversion"
    }

    fn try_rewrite(&self, _expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        // Disabled — see struct docs.
        None
    }
}

// ── R10: WindowMerge ─────────────────────────────────────────────────────────

/// Merge two adjacent identical `Window` nodes into one.
///
/// `Window(w, Window(w, e))` → `Window(w, e)`
pub struct WindowMerge;

impl RewriteRule for WindowMerge {
    fn name(&self) -> &'static str {
        "WindowMerge"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            // `TimeRange` is the canonical range-vector-selector node (see
            // control_plane/docs/design-asapplanner-pin-migration.md) --
            // was `Window { kind, size, slide, child }`; the identity-merge
            // rule collapses to comparing `range` alone now that there's no
            // separate kind/slide to also agree on.
            QueryExpr::TimeRange { range, child } => {
                if let QueryExpr::TimeRange {
                    range: inner_range,
                    child: inner_child,
                } = (*child).clone()
                {
                    if range == inner_range {
                        return Some(QueryExpr::TimeRange {
                            range,
                            child: inner_child,
                        });
                    }
                }
                None
            }
            _ => None,
        }
    }
}

// R11 (retired): `PartitionElim` removed a `Partition` node with an empty
// key list. `Partition` no longer exists in the canonical IR — its keys
// fold into `Aggregate.by: GroupKeys` at construction time
// (`intent_algebra::lower`), so an empty-keys `Partition` never gets
// built in the first place; nothing left for this rule to eliminate.

// ── R12: SetOpFusion ──────────────────────────────────────────────────────────

/// Fuse `SetOp(Union ALL, Merge([…]), Merge([…]))` into a single `Merge([…, …])`.
pub struct SetOpFusion;

impl RewriteRule for SetOpFusion {
    fn name(&self) -> &'static str {
        "SetOpFusion"
    }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::SetOp {
                kind: SetOpKind::Union,
                all: true,
                left,
                right,
            } => match ((*left).clone(), (*right).clone()) {
                (
                    QueryExpr::Concat { children: mut lc },
                    QueryExpr::Concat { children: mut rc },
                ) => {
                    lc.append(&mut rc);
                    Some(QueryExpr::Concat { children: lc })
                }
                (l, r) => Some(QueryExpr::SetOp {
                    kind: SetOpKind::Union,
                    all: true,
                    left: Rc::new(l),
                    right: Rc::new(r),
                }),
            },
            _ => None,
        }
    }
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

/// Fixed-point query optimizer.
///
/// Call [`QueryOptimizer::optimize`] to rewrite a canonical [`QueryExpr`]
/// tree. The optimizer iterates over all registered rules until no rule fires.
pub struct QueryOptimizer {
    rules: Vec<Box<dyn RewriteRule>>,
    cost_model: Box<dyn CostModel>,
    /// Maximum number of fixed-point iterations (prevents infinite loops).
    max_iters: usize,
}

impl QueryOptimizer {
    /// Create an optimizer with the default rule set and cost model.
    pub fn new(raw_bytes_per_sec: f64) -> Self {
        Self {
            rules: default_rules(),
            cost_model: Box::new(DefaultCostModel {
                raw_bytes_per_sec,
                deployment: None,
            }),
            max_iters: 32,
        }
    }

    /// Create an optimizer with deployment constraints.
    pub fn with_constraints(raw_bytes_per_sec: f64, constraints: DeploymentConstraints) -> Self {
        Self {
            rules: default_rules(),
            cost_model: Box::new(DefaultCostModel {
                raw_bytes_per_sec,
                deployment: Some(constraints),
            }),
            max_iters: 32,
        }
    }

    /// Create an optimizer with a custom cost model.
    pub fn with_cost_model(cost_model: Box<dyn CostModel>) -> Self {
        Self {
            rules: default_rules(),
            cost_model,
            max_iters: 32,
        }
    }

    /// Set maximum fixed-point iterations (default: 32).
    pub fn max_iters(mut self, n: usize) -> Self {
        self.max_iters = n;
        self
    }

    /// Optimize `expr` until fixed point or `max_iters` iterations.
    ///
    /// Returns the rewritten tree and the number of iterations actually run.
    pub fn optimize(&self, expr: QueryExpr) -> (QueryExpr, usize) {
        let mut current = expr;
        for iter in 0..self.max_iters {
            let (next, changed) = self.apply_all(current);
            current = next;
            if !changed {
                return (current, iter + 1);
            }
        }
        (current, self.max_iters)
    }

    /// Apply all rules once to every node in the tree (single pass).
    /// Returns `(new_tree, did_anything_change)`.
    fn apply_all(&self, expr: QueryExpr) -> (QueryExpr, bool) {
        // First recurse into children, then try rules at this node.
        let (expr_with_new_children, child_changed) = self.recurse_children(expr);
        let (final_expr, this_changed) = self.apply_rules_at(expr_with_new_children);
        (final_expr, child_changed || this_changed)
    }

    /// Apply all rules at the current node (no recursion).
    fn apply_rules_at(&self, mut expr: QueryExpr) -> (QueryExpr, bool) {
        let mut changed = false;
        for rule in &self.rules {
            if let Some(new_expr) = rule.try_rewrite(expr.clone(), self.cost_model.as_ref()) {
                expr = new_expr;
                changed = true;
                // After firing, restart from the first rule (fixed-point per node).
                break;
            }
        }
        (expr, changed)
    }

    /// Recurse into children, rebuilding the node with rewritten children.
    fn recurse_children(&self, expr: QueryExpr) -> (QueryExpr, bool) {
        macro_rules! recurse {
            ($child:expr) => {{
                let (e, c) = self.apply_all((*$child).clone());
                (Rc::new(e), c)
            }};
        }
        match expr {
            // `Ref` doesn't exist in the canonical `QueryExpr` anymore
            // (see `CommonSubexprElim`'s doc) -- `Scan` is the only leaf
            // that still matches here unchanged.
            QueryExpr::Scan { .. } => (expr, false),

            QueryExpr::Filter { pred, child } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::Filter {
                        pred,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::Project {
                cols,
                qualifier,
                child,
            } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::Project {
                        cols,
                        qualifier,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::Aggregate {
                reduction,
                measures: aggs,
                output_names,
                having,
                child,
            } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::Aggregate {
                        reduction,
                        measures: aggs,
                        output_names,
                        having,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::TimeRange { range, child } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::TimeRange {
                        range,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::Dedup { cols, child } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::Dedup {
                        cols,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::Sort {
                keys,
                partition_by,
                child,
            } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::Sort {
                        keys,
                        partition_by,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::Limit { n, offset, child } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::Limit {
                        n,
                        offset,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::PromqlSubquery {
                range,
                resolution,
                child,
            } => {
                let (new_child, c) = recurse!(child);
                (
                    QueryExpr::PromqlSubquery {
                        range,
                        resolution,
                        child: new_child,
                    },
                    c,
                )
            }
            QueryExpr::Concat { children } => {
                let (new_children, changed): (Vec<_>, Vec<_>) =
                    children.into_iter().map(|inp| self.apply_all(inp)).unzip();
                (
                    QueryExpr::Concat {
                        children: new_children,
                    },
                    changed.into_iter().any(|c| c),
                )
            }
            QueryExpr::Join {
                kind,
                pred,
                left,
                right,
            } => {
                let (new_left, cl) = recurse!(left);
                let (new_right, cr) = recurse!(right);
                (
                    QueryExpr::Join {
                        kind,
                        pred,
                        left: new_left,
                        right: new_right,
                    },
                    cl || cr,
                )
            }
            QueryExpr::SetOp {
                kind,
                all,
                left,
                right,
            } => {
                let (new_left, cl) = recurse!(left);
                let (new_right, cr) = recurse!(right);
                (
                    QueryExpr::SetOp {
                        kind,
                        all,
                        left: new_left,
                        right: new_right,
                    },
                    cl || cr,
                )
            }
            QueryExpr::BinaryOp {
                op,
                lhs,
                rhs,
                vector_match,
            } => {
                let (new_lhs, cl) = recurse!(lhs);
                let (new_rhs, cr) = recurse!(rhs);
                (
                    QueryExpr::BinaryOp {
                        op,
                        lhs: new_lhs,
                        rhs: new_rhs,
                        vector_match,
                    },
                    cl || cr,
                )
            }
            // `LetBinding` doesn't exist in the canonical `QueryExpr`
            // anymore -- see `CommonSubexprElim`'s doc.
            // `asap_ir`'s `QueryExpr` superset (Scalar / EvalTime /
            // VectorFromScalar / ScalarFromVector / Relabel / InfoJoin /
            // Sample / TimeRange / TimeShift / WindowFunc) — PromQL
            // surface no rule here targets yet. Treated as opaque leaves:
            // returned unchanged rather than recursed into, so a rewrite
            // opportunity nested inside one of these is missed rather
            // than mishandled. Not a correctness issue (the optimizer is
            // a pure best-effort rewrite pass — leaving a subtree
            // unrewritten is always semantically safe), just a
            // known coverage gap to close when a rule needs to see
            // inside them.
            other => (other, false),
        }
    }
}

/// Construct the default ordered rule set.
fn default_rules() -> Vec<Box<dyn RewriteRule>> {
    vec![
        Box::new(PredicatePushDown),
        Box::new(FilterWindowSwap),
        Box::new(HLLDedupElim),
        Box::new(WindowMerge),
        Box::new(TopKFusion),
        // R6 (HistogramQuantileFusion) retired in Step γ5.
        Box::new(MergeLifting),
        Box::new(SetOpFusion),
        Box::new(HydraConversion),
        // R7 (SubqueryDecorrelation) and R11 (PartitionElim) retired — see
        // the doc-table notes above.
        Box::new(CommonSubexprElim),
    ]
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::pre_asap::{default_cardinality, default_quantile};
    use planner_types::pre_asap::{BinaryOpKind, QueryExpr, ScalarValue, Schema, SortKey, Source};
    use planner_types::pre_asap::{GroupKeys, Predicate};
    use std::time::Duration;

    /// Canonical `Scan` leaf.
    fn scan(name: &str) -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: name.into(),
            },
            predicates: vec![],
            schema: Schema::default(),
        }
    }

    /// Single-intent, global, no-HAVING `Aggregate` — the canonical fold of
    /// the legacy `SketchAgg`.
    fn sketch_agg(intent: AggIntent, child: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![intent],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(child),
        }
    }

    fn true_pred() -> Predicate {
        Predicate(Rc::new(QueryExpr::Literal(ScalarValue::Boolean(true))))
    }

    fn opt() -> QueryOptimizer {
        QueryOptimizer::new(100_000.0)
    }

    /// Recursively check whether the tree contains any `Distinct` node.
    /// Only walks the variants these tests actually build (`Scan`/`Ref`,
    /// `Filter`/`Project`/`Aggregate`/`Window`/`Sort`/`Limit`/`Subquery`,
    /// `Merge`, `Join`/`SetOp`/`BinaryOp`, `LetBinding`, `Distinct`) — the
    /// PromQL-only leaves (`Scalar`, `EvalTime`, `Relabel`, …) never appear
    /// in a tree these helpers construct.
    fn contains_distinct(qe: &QueryExpr) -> bool {
        match qe {
            QueryExpr::Dedup { .. } => true,
            QueryExpr::Filter { child, .. }
            | QueryExpr::Project { child, .. }
            | QueryExpr::Aggregate { child, .. }
            | QueryExpr::TimeRange { child, .. }
            | QueryExpr::Sort { child, .. }
            | QueryExpr::Limit { child, .. }
            | QueryExpr::PromqlSubquery { child, .. } => contains_distinct(child),
            QueryExpr::Concat { children } => children.iter().any(contains_distinct),
            QueryExpr::Join { left, right, .. }
            | QueryExpr::SetOp { left, right, .. }
            | QueryExpr::BinaryOp {
                lhs: left,
                rhs: right,
                ..
            } => contains_distinct(left) || contains_distinct(right),
            _ => false,
        }
    }

    // ── R1: PredicatePushDown ─────────────────────────────────────────────────

    #[test]
    fn r1_pushes_filter_below_window() {
        let expr = QueryExpr::Filter {
            pred: true_pred(),
            child: Rc::new(QueryExpr::TimeRange {
                range: Duration::from_secs(60),
                child: Rc::new(scan("m")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::TimeRange { child, .. }
                if matches!(child.as_ref(), QueryExpr::Filter { .. })),
            "filter should be inside window: {result:?}"
        );
    }

    #[test]
    fn r1_pushes_filter_below_sort() {
        let expr = QueryExpr::Filter {
            pred: true_pred(),
            child: Rc::new(QueryExpr::Sort {
                keys: vec![SortKey {
                    expr: QueryExpr::Column(0),
                    ascending: true,
                    nulls_first: false,
                }],
                partition_by: GroupKeys::none(),
                child: Rc::new(scan("cpu")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(matches!(&result, QueryExpr::Sort { child, .. }
            if matches!(child.as_ref(), QueryExpr::Filter { .. })));
    }

    // ── R3: HLLDedupElim ─────────────────────────────────────────────────────

    #[test]
    fn r3_removes_dedup_before_hll() {
        let expr = sketch_agg(
            default_cardinality(),
            QueryExpr::Dedup {
                cols: vec![0],
                child: Rc::new(scan("events")),
            },
        );
        let (result, _) = opt().optimize(expr);
        assert!(
            !contains_distinct(&result),
            "Distinct should be eliminated before HLL: {result:?}"
        );
    }

    // ── R5: TopKFusion ────────────────────────────────────────────────────────

    #[test]
    fn r5_fuses_limit_sort_to_topk() {
        let expr = QueryExpr::Limit {
            n: 10,
            offset: 0,
            child: Rc::new(QueryExpr::Sort {
                keys: vec![SortKey {
                    expr: QueryExpr::Column(0),
                    ascending: false,
                    nulls_first: false,
                }],
                partition_by: GroupKeys::none(),
                child: Rc::new(scan("events")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::Aggregate { measures: aggs, .. }
                if matches!(aggs.as_slice(), [AggIntent::TopK { k: 10, .. }])),
            "expected Aggregate[TopK(10)], got {result:?}"
        );
    }

    #[test]
    fn r5_does_not_fuse_ascending_sort() {
        let expr = QueryExpr::Limit {
            n: 10,
            offset: 0,
            child: Rc::new(QueryExpr::Sort {
                keys: vec![SortKey {
                    expr: QueryExpr::Column(0),
                    ascending: true,
                    nulls_first: false,
                }],
                partition_by: GroupKeys::none(),
                child: Rc::new(scan("events")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            !matches!(&result, QueryExpr::Aggregate { measures: aggs, .. }
                if matches!(aggs.as_slice(), [AggIntent::TopK { .. }])),
            "ascending sort should not become a TopK aggregate"
        );
    }

    // ── R10: WindowMerge ──────────────────────────────────────────────────────

    #[test]
    fn r10_merges_duplicate_windows() {
        let expr = QueryExpr::TimeRange {
            range: Duration::from_secs(300),
            child: Rc::new(QueryExpr::TimeRange {
                range: Duration::from_secs(300),
                child: Rc::new(scan("m")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::TimeRange { child, .. }
                if !matches!(child.as_ref(), QueryExpr::TimeRange { .. })),
            "duplicate window should be merged: {result:?}"
        );
    }

    // ── Fixed-point convergence ───────────────────────────────────────────────

    #[test]
    fn optimizer_reaches_fixed_point_on_simple_tree() {
        let (result, iters) = opt().optimize(scan("m"));
        assert!(iters < 5, "should converge quickly on scan-only tree");
        assert!(matches!(result, QueryExpr::Scan { .. }));
    }

    #[test]
    fn optimizer_chain_of_rewrites() {
        // Filter(Window(Aggregate[Cardinality](Distinct(Scan)))) →
        //   R1: Window(Filter(Aggregate[Cardinality](Distinct(Scan))))
        //   R3: Window(Filter(Aggregate[Cardinality](Scan)))
        let expr = QueryExpr::Filter {
            pred: true_pred(),
            child: Rc::new(QueryExpr::TimeRange {
                range: Duration::from_secs(60),
                child: Rc::new(sketch_agg(
                    default_cardinality(),
                    QueryExpr::Dedup {
                        cols: vec![0],
                        child: Rc::new(scan("events")),
                    },
                )),
            }),
        };
        let (result, _iters) = opt().optimize(expr);
        assert!(
            !contains_distinct(&result),
            "Distinct should have been eliminated: {result:?}"
        );
    }

    // ── R2: MergeLifting ─────────────────────────────────────────────────────

    #[test]
    fn r2_lifts_mergeable_sketch_above_merge() {
        let expr = sketch_agg(
            default_cardinality(),
            QueryExpr::Concat {
                children: vec![scan("shard_a"), scan("shard_b")],
            },
        );
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::Concat { children }
                if children.iter().all(|c| matches!(c, QueryExpr::Aggregate { .. }))),
            "cardinality aggregate should be pushed into each Merge branch: {result:?}"
        );
    }

    // ── R12: SetOpFusion ─────────────────────────────────────────────────────

    #[test]
    fn r12_fuses_union_of_merges() {
        let expr = QueryExpr::SetOp {
            kind: SetOpKind::Union,
            all: true,
            left: Rc::new(QueryExpr::Concat {
                children: vec![scan("a"), scan("b")],
            }),
            right: Rc::new(QueryExpr::Concat {
                children: vec![scan("c")],
            }),
        };
        let (result, _) = opt().optimize(expr);
        match result {
            QueryExpr::Concat { children } => assert_eq!(children.len(), 3),
            other => panic!("expected Merge(3), got {other:?}"),
        }
    }

    // ── R8: CommonSubexprElim (disabled -- see the rule's own doc) ───────────

    #[test]
    fn r8_no_longer_hoists_repeated_scan() {
        // `CommonSubexprElim` always returns `None` now (no `LetBinding`/
        // `Ref` to hoist into) -- the tree comes back unchanged rather
        // than with duplicate scans hoisted into a shared binding.
        let expr = QueryExpr::Concat {
            children: vec![scan("dup"), scan("dup"), scan("other")],
        };
        let (result, _) = opt().optimize(expr);
        match result {
            QueryExpr::Concat { children } => assert_eq!(children.len(), 3),
            other => panic!("expected Merge(3), got {other:?}"),
        }
    }

    // ── DeploymentConstraints tests ─────────────────────────────────────

    #[test]
    fn constraints_from_budgets() {
        let budgets = crate::types::StageResourceBudgets {
            agent_memory_bytes: Some(4096),
            backend_memory_bytes: Some(1_000_000),
            ..Default::default()
        };
        let dc = DeploymentConstraints::from_budgets(&budgets);
        assert_eq!(dc.agent.memory_bytes, Some(4096));
        assert_eq!(dc.backend_collector.memory_bytes, Some(1_000_000));
    }

    #[test]
    fn constrained_optimizer_penalises_large_sketch() {
        let dc = DeploymentConstraints {
            agent: StageBudget {
                memory_bytes: Some(1),
                ..Default::default()
            },
            ..Default::default()
        };
        let opt = QueryOptimizer::with_constraints(1000.0, dc);
        let expr = sketch_agg(default_quantile(0.99), scan("m"));
        let cost = opt.cost_model.estimate(&expr);
        // Memory should be heavily penalised (10× multiplier).
        assert!(
            cost.memory_bytes > 10_000.0,
            "expected penalised memory, got {}",
            cost.memory_bytes
        );
    }

    #[test]
    fn unconstrained_optimizer_normal_cost() {
        let opt = QueryOptimizer::new(1000.0);
        let expr = sketch_agg(default_quantile(0.99), scan("m"));
        let cost = opt.cost_model.estimate(&expr);
        assert!(
            cost.memory_bytes < 10_000.0,
            "expected normal memory, got {}",
            cost.memory_bytes
        );
    }

    // Silence unused-import warnings for items only used in some test configs.
    #[allow(dead_code)]
    fn _import_anchors(_: BinaryOpKind) {}
}
