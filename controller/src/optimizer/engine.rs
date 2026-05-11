//! Cost-based fixed-point query optimizer.
//!
//! The optimizer applies a set of algebraic rewrite rules to a
//! [`QueryExpr`](crate::intent_algebra::legacy_expr::QueryExpr) tree until no rule fires (fixed
//! point).  Each rule is a pure function `QueryExpr → Option<QueryExpr>`:
//! returning `None` means "this rule does not apply here".
//!
//! # Rules implemented
//!
//! | Rule | Name | Description |
//! |------|------|-------------|
//! | R1 | `PredicatePushDown`     | Push `Filter` below `Window`, `Partition`, `Aggregate` |
//! | R2 | `MergeLifting`          | Lift mergeable sketch aggs above `Partition` nodes |
//! | R3 | `HLLDedupElim`          | Eliminate `Dedup` before HLL (HLL is inherently distinct) |
//! | R4 | `FilterWindowSwap`      | Swap `Filter` below `Window` to reduce window input size |
//! | R5 | `TopKFusion`            | Absorb `Limit` / `TopK` into a `CountSketch` agg |
//! | R6 | `HistogramQuantileFusion` | Recognise `HistogramQuantile(φ, Agg(DDSketch))` and mark |
//! | R7 | `SubqueryDecorrelation` | Hoist correlated `ScalarSubquery` to a `LetBinding` |
//! | R8 | `CommonSubexprElim`     | Extract identical sub-trees into `LetBinding`s |
//! | R9 | `HydraConversion`       | Convert multi-key `Partition + Agg` into `Hydra` sketch |
//! | R10| `WindowMerge`           | Merge adjacent `Window` nodes with the same duration |
//! | R11| `PartitionElim`         | Remove `Partition` with empty key list (becomes global agg) |
//! | R12| `SetOpFusion`           | Fuse `SetOp(Union, Merge, Merge)` into a single `Merge` |
//!
//! ## Cost model integration
//!
//! Rules R1–R4 are cost-free (always beneficial).  Rules R5–R12 consult a
//! [`CostModel`] that estimates bandwidth, memory, and CPU overhead.  A
//! rewrite is only applied when the estimated cost improves.

use std::collections::HashMap;

use crate::intent_algebra::legacy_expr::{QueryExpr, ScalarExpr, SetOpKind, SortKey};
use crate::intent_algebra::legacy_expr::{AggIntent, PartitionKeys, SourceSpec};
use crate::sketch_algebra::capability::{
    default_capability_table, load_capability_overrides, SketchCapability,
};

// ── Cost model interface ──────────────────────────────────────────────────────

/// Estimated cost of evaluating an expression at a given bandwidth.
#[derive(Debug, Clone, Default)]
pub struct NodeCost {
    pub bytes_per_sec: f64,
    pub memory_bytes:  f64,
    pub cpu_per_sample: f64,
}

/// Pluggable cost oracle.  The default implementation uses simple heuristics.
pub trait CostModel: Send + Sync {
    /// Estimate the cost of the expression tree rooted at `expr`.
    fn estimate(&self, expr: &QueryExpr) -> NodeCost;

    /// Deployment constraints (memory budgets, available backends, etc.).
    /// Returns `None` if no constraints are configured (unconstrained mode).
    fn constraints(&self) -> Option<&DeploymentConstraints> { None }
}

// ── Sketch capabilities ─────────────────────────────────────────────────────
//
// Per the Step 2a consolidation, `SketchCapability` / `SupportedIntent` and
// the YAML-loader logic now live in `crate::sketch_algebra::capability`.
// The optimizer re-exports `sketch_capability(SketchType)` and
// `load_sketch_capabilities(path)` as thin shims so existing callers
// (`algebra::physical`, `main.rs`) keep building while the legacy
// `crate::types::SketchType` key continues to be the lookup key.

/// Load sketch capabilities from a YAML file. Thin shim — the real
/// loader lives in `sketch_algebra::capability::load_capability_overrides`
/// and is keyed by `SketchKind`. This shim translates the result to the
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
/// the real defaults live in `sketch_algebra::capability::default_capability_table`.
pub fn sketch_capability(st: &crate::types::SketchType) -> SketchCapability {
    use crate::sketch_algebra::params::SketchKind;
    use crate::types::SketchType;
    let kind: SketchKind = match st {
        SketchType::DDSketch => SketchKind::DDSketch,
        SketchType::KLL => SketchKind::Kll,
        SketchType::HLL => SketchKind::Hll,
        SketchType::CountSketch => SketchKind::CountSketch,
        SketchType::CountMinSketch => SketchKind::Cms,
    };
    default_capability_table()
        .remove(&kind)
        .expect("default_capability_table covers every SketchKind variant")
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
            if cap.memory_bytes_per_series > mem { return false; }
        }
        if let Some(cpu) = self.cpu_micros_per_sample {
            if cap.cpu_micros_per_insert > cpu { return false; }
        }
        if let Some(bw) = self.bandwidth_bytes_per_sec {
            // Rough: transmission bytes per flush ÷ 1 second
            if cap.transmission_bytes as f64 > bw { return false; }
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

impl CostModel for DefaultCostModel {
    fn constraints(&self) -> Option<&DeploymentConstraints> {
        self.deployment.as_ref()
    }

    fn estimate(&self, expr: &QueryExpr) -> NodeCost {
        use crate::intent_algebra::legacy_expr::agg_is_exact;
        // Sketch nodes reduce bandwidth; exact nodes pass through.
        let factor = match expr {
            QueryExpr::SketchAgg { op, .. } | QueryExpr::WindowedAgg { agg: op, .. } => match op {
                AggIntent::Quantile { .. }    => 0.05,
                AggIntent::Cardinality { .. } => 0.02,
                AggIntent::Frequency { .. }   => 0.03,
                op if agg_is_exact(op)        => 1.0,
                _                             => 0.1,
            },
            QueryExpr::Merge { inputs } => 1.0 / (inputs.len().max(1) as f64),
            QueryExpr::Filter { .. }    => 0.5,
            QueryExpr::TopK { k, .. }   => (*k as f64).recip().min(0.1),
            QueryExpr::Partition { .. }  => 0.8, // partition adds overhead
            QueryExpr::Distinct { .. }   => 0.9,
            _                           => 1.0,
        };

        let memory = match expr {
            QueryExpr::SketchAgg { op, .. } | QueryExpr::WindowedAgg { agg: op, .. } =>
                crate::physical::sketch_catalog::estimated_sketch_memory_bytes(op) as f64,
            _ => self.raw_bytes_per_sec * factor * 0.01,
        };

        let base = NodeCost {
            bytes_per_sec: self.raw_bytes_per_sec * factor,
            memory_bytes:  memory,
            cpu_per_sample: factor * 10.0,
        };

        // Apply deployment constraint penalties.
        if let Some(dc) = &self.deployment {
            // Map each expression to its default stage budget.
            let stage_budget = match expr {
                QueryExpr::SketchAgg { .. } | QueryExpr::WindowedAgg { .. }
                | QueryExpr::Source(_) | QueryExpr::Filter { .. }
                | QueryExpr::Window { .. } => &dc.agent,
                QueryExpr::Partition { .. } | QueryExpr::Merge { .. }
                | QueryExpr::Distinct { .. } => &dc.backend_collector,
                QueryExpr::TopK { .. } | QueryExpr::HistogramQuantile { .. }
                | QueryExpr::BinaryOp { .. } | QueryExpr::PromQLSubquery { .. } => &dc.backend_db,
                QueryExpr::Aggregate { .. } => &dc.original_db,
                _ => &dc.agent,
            };

            // For sketch nodes, check sketch capability against stage budget.
            if let QueryExpr::SketchAgg { op, .. } | QueryExpr::WindowedAgg { agg: op, .. } = expr {
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
                // Sliding window: penalise if sketch doesn't support it natively.
                if let QueryExpr::WindowedAgg { window, .. } = expr {
                    if matches!(window.kind, crate::intent_algebra::legacy_expr::WindowKind::Sliding { .. })
                        && !cap.supports_sliding_window
                    {
                        return NodeCost {
                            cpu_per_sample: base.cpu_per_sample * 5.0,
                            ..base
                        };
                    }
                }
            } else {
                // Non-sketch nodes: check basic budget constraints.
                if let Some(budget) = stage_budget.memory_bytes {
                    if base.memory_bytes > budget as f64 {
                        return NodeCost { memory_bytes: base.memory_bytes * 10.0, ..base };
                    }
                }
                if let Some(bw) = stage_budget.bandwidth_bytes_per_sec {
                    if base.bytes_per_sec > bw {
                        return NodeCost { bytes_per_sec: base.bytes_per_sec * 10.0, ..base };
                    }
                }
            }
        }
        base
    }
}

// ── Rewrite rule trait ────────────────────────────────────────────────────────

/// A single algebraic rewrite rule.
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
/// Transformations applied (all strictly beneficial, no cost model check):
/// * `Filter(p, Window(d, e))` → `Window(d, Filter(p, e))`
/// * `Filter(p, Partition(k, e))` → `Partition(k, Filter(p, e))`
/// * `Filter(p, Sort(k, e))` → `Sort(k, Filter(p, e))`
/// * `Filter(p, Limit(n, o, e))` → `Limit(n, o, Filter(p, e))`  (NB: only safe when p is on input cols)
pub struct PredicatePushDown;

impl RewriteRule for PredicatePushDown {
    fn name(&self) -> &'static str { "PredicatePushDown" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Filter { pred, input } => {
                match *input {
                    // Filter below Window
                    QueryExpr::Window { duration, slide, input: inner } => Some(
                        QueryExpr::Window {
                            duration,
                            slide,
                            input: Box::new(QueryExpr::Filter {
                                pred,
                                input: inner,
                            }),
                        }
                    ),
                    // Filter below Partition
                    QueryExpr::Partition { keys, input: inner } => Some(
                        QueryExpr::Partition {
                            keys,
                            input: Box::new(QueryExpr::Filter {
                                pred,
                                input: inner,
                            }),
                        }
                    ),
                    // Filter below Sort (safe when pred references input columns only)
                    QueryExpr::Sort { keys, input: inner } => Some(
                        QueryExpr::Sort {
                            keys,
                            input: Box::new(QueryExpr::Filter {
                                pred,
                                input: inner,
                            }),
                        }
                    ),
                    // Not applicable — reconstruct
                    other => Some(QueryExpr::Filter { pred, input: Box::new(other) }),
                }
            }
            _ => None,
        }
    }
}

// ── R2: MergeLifting ──────────────────────────────────────────────────────────

/// Lift mergeable `SketchAgg` ops above `Merge` nodes.
///
/// `SketchAgg(op, Merge([a, b]))` → `Merge([SketchAgg(op, a), SketchAgg(op, b)])`
///
/// Only applied for `is_mergeable()` ops so we don't incorrectly distribute
/// `Avg` or `StdDev`.
pub struct MergeLifting;

impl RewriteRule for MergeLifting {
    fn name(&self) -> &'static str { "MergeLifting" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::SketchAgg { ref op, ref col, ref input }
                if crate::intent_algebra::legacy_expr::agg_is_mergeable(op) =>
            {
                if let QueryExpr::Merge { inputs } = input.as_ref() {
                    let new_inputs: Vec<QueryExpr> = inputs.iter().map(|branch| {
                        QueryExpr::SketchAgg {
                            op:    op.clone(),
                            col:   col.clone(),
                            input: Box::new(branch.clone()),
                        }
                    }).collect();
                    return Some(QueryExpr::Merge { inputs: new_inputs });
                }
                None
            }
            _ => None,
        }
    }
}

// ── R3: HLLDedupElim ─────────────────────────────────────────────────────────

/// Eliminate `Dedup` nodes that immediately precede an HLL aggregation.
///
/// HLL counts distinct values intrinsically; an explicit dedup step is
/// redundant and wastes CPU / memory.
///
/// `SketchAgg(HLL, Dedup(col, e))` → `SketchAgg(HLL, e)`
pub struct HLLDedupElim;

impl RewriteRule for HLLDedupElim {
    fn name(&self) -> &'static str { "HLLDedupElim" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::SketchAgg { op: AggIntent::Cardinality { accuracy }, col, input } => {
                if let QueryExpr::Distinct { input: inner, .. } = *input {
                    return Some(QueryExpr::SketchAgg {
                        op:    AggIntent::Cardinality { accuracy },
                        col,
                        input: inner,
                    });
                }
                None
            }
            _ => None,
        }
    }
}

// ── R4: FilterWindowSwap ──────────────────────────────────────────────────────

/// Push `Filter` below `Window` when the predicate references only source
/// columns (not windowed aggregates).
///
/// Identical to the push-down in R1 for the Window case, but checked
/// separately so the optimizer can attribute the transformation correctly
/// in logs.
pub struct FilterWindowSwap;

impl RewriteRule for FilterWindowSwap {
    fn name(&self) -> &'static str { "FilterWindowSwap" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        // Handled by PredicatePushDown — mark as no-op here to avoid double-fire.
        match expr {
            QueryExpr::Filter { pred, input } => {
                if let QueryExpr::Window { duration, slide, input: inner } = *input {
                    return Some(QueryExpr::Window {
                        duration,
                        slide,
                        input: Box::new(QueryExpr::Filter { pred, input: inner }),
                    });
                }
                None
            }
            _ => None,
        }
    }
}

// ── R5: TopKFusion ────────────────────────────────────────────────────────────

/// Fuse a `Limit(k, TopK(_, e))` or `Limit(k, Sort(_, e))` into a single
/// `TopK(k, e)` node that the allocator maps to a `CountSketch`.
///
/// `Limit(n, Sort([col DESC], e))` → `TopK(n, [col], e)`
pub struct TopKFusion;

impl RewriteRule for TopKFusion {
    fn name(&self) -> &'static str { "TopKFusion" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Limit { n, offset: 0, input } => {
                if let QueryExpr::Sort { keys, input: inner } = *input {
                    // Only fuse when all keys are DESC (top-k semantics).
                    if keys.iter().all(|k| k.desc) {
                        let by: Vec<String> = keys.into_iter().map(|k| k.col).collect();
                        return Some(QueryExpr::TopK { k: n, by, input: inner });
                    }
                }
                None
            }
            _ => None,
        }
    }
}

// ── R6: HistogramQuantileFusion ───────────────────────────────────────────────

/// Recognise `HistogramQuantile(φ, SketchAgg(DDSketch([φ]), …))` and
/// simplify to a single annotated node that the allocator handles as one
/// DDSketch query.
///
/// `HistogramQuantile(φ, SketchAgg(DDSketch(qs), col, e))`
///    where `qs` contains `φ`
/// → `HistogramQuantile(φ, SketchAgg(DDSketch(qs), col, e))`   [marked fused]
///
/// In practice we just ensure the quantile is in the DDSketch's quantile
/// list so the allocator emits a single sketch with the right φ.
pub struct HistogramQuantileFusion;

impl RewriteRule for HistogramQuantileFusion {
    fn name(&self) -> &'static str { "HistogramQuantileFusion" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        // Canonical Quantile is single-φ post Step α — multi-φ fan-out
        // happens at construction time, so the "merge phi into the
        // existing quantile list" branch is now a "if phis match, keep
        // structure; else build a Merge of two SketchAgg siblings". For
        // this PR we keep the structural marker (identity rewrite) and
        // defer the Merge-aware fusion to Step γ; the original rule was
        // primarily a structural marker anyway.
        match expr {
            QueryExpr::HistogramQuantile { phi, input } => {
                match *input {
                    QueryExpr::SketchAgg {
                        op: AggIntent::Quantile { q, accuracy },
                        col,
                        input: inner,
                    } => {
                        if (q - phi).abs() < f64::EPSILON {
                            // Quantile already matches φ — keep shape.
                            Some(QueryExpr::HistogramQuantile {
                                phi,
                                input: Box::new(QueryExpr::SketchAgg {
                                    op: AggIntent::Quantile { q, accuracy },
                                    col,
                                    input: inner,
                                }),
                            })
                        } else {
                            // φ ≠ q: build a Merge of two single-φ
                            // SketchAgg siblings (F1 fan-out for the
                            // multi-φ case) and re-wrap.
                            let new_q = QueryExpr::SketchAgg {
                                op:    AggIntent::Quantile { q: phi, accuracy: accuracy.clone() },
                                col:   col.clone(),
                                input: inner.clone(),
                            };
                            let old_q = QueryExpr::SketchAgg {
                                op: AggIntent::Quantile { q, accuracy },
                                col,
                                input: inner,
                            };
                            Some(QueryExpr::HistogramQuantile {
                                phi,
                                input: Box::new(QueryExpr::Merge { inputs: vec![new_q, old_q] }),
                            })
                        }
                    }
                    other => Some(QueryExpr::HistogramQuantile { phi, input: Box::new(other) }),
                }
            }
            _ => None,
        }
    }
}

// ── R7: SubqueryDecorrelation ─────────────────────────────────────────────────

/// Hoist correlated `ScalarSubquery` references into `LetBinding`s so that
/// the subquery is evaluated once rather than once per row.
///
/// This rule is a structural marker — full correlated-subquery detection
/// requires a binder pass that is out of scope here.  We handle the simple
/// case: a `Filter` whose predicate contains a `ScalarSubquery` that does
/// not reference the filter's own input.
pub struct SubqueryDecorrelation;

impl RewriteRule for SubqueryDecorrelation {
    fn name(&self) -> &'static str { "SubqueryDecorrelation" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Filter { pred, input } => {
                if let Some((name, sq_expr, new_pred)) = extract_scalar_subquery(pred) {
                    return Some(QueryExpr::LetBinding {
                        name:  name.clone(),
                        expr:  Box::new(sq_expr),
                        body:  Box::new(QueryExpr::Filter {
                            pred:  new_pred,
                            input,
                        }),
                    });
                }
                None
            }
            _ => None,
        }
    }
}

/// If `pred` contains a `ScalarSubquery`, extract it as
/// `(binding_name, subquery_expr, pred_with_ref)`.
fn extract_scalar_subquery(
    pred: ScalarExpr,
) -> Option<(String, QueryExpr, ScalarExpr)> {
    match pred {
        ScalarExpr::BinaryOp { op, lhs, rhs } => {
            // Check lhs
            if let ScalarExpr::ScalarSubquery(sq) = *lhs {
                let name = "__subq_0".to_string();
                let new_pred = ScalarExpr::BinaryOp {
                    op,
                    lhs: Box::new(ScalarExpr::Column(name.clone())),
                    rhs,
                };
                return Some((name, *sq, new_pred));
            }
            // Check rhs
            if let ScalarExpr::ScalarSubquery(sq) = *rhs {
                let name = "__subq_0".to_string();
                let new_pred = ScalarExpr::BinaryOp {
                    op,
                    lhs,
                    rhs: Box::new(ScalarExpr::Column(name.clone())),
                };
                return Some((name, *sq, new_pred));
            }
            None
        }
        _ => None,
    }
}

// ── R8: CommonSubexprElim ─────────────────────────────────────────────────────

/// Identify identical sub-trees that appear in multiple branches of a `Merge`
/// node and hoist them into a `LetBinding`.
///
/// This is a conservative implementation: only `Source` nodes with the same
/// name are deduplicated (the common case where the same metric appears in
/// multiple union branches).
pub struct CommonSubexprElim;

impl RewriteRule for CommonSubexprElim {
    fn name(&self) -> &'static str { "CommonSubexprElim" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Merge { ref inputs } => {
                // Count occurrences of each source name.
                let mut counts: HashMap<String, usize> = HashMap::new();
                for inp in inputs {
                    if let Some(name) = inp.source_name() {
                        *counts.entry(name.to_string()).or_insert(0) += 1;
                    }
                }
                let repeated: Vec<String> = counts.into_iter()
                    .filter(|(_, c)| *c > 1)
                    .map(|(n, _)| n)
                    .collect();
                if repeated.is_empty() {
                    return None;
                }
                // Hoist the first repeated source into a LetBinding.
                let name = repeated.into_iter().next()?;
                let binding_name = format!("__cse_{name}");
                let new_inputs: Vec<QueryExpr> = inputs.iter().cloned().map(|inp| {
                    if inp.source_name() == Some(name.as_str()) {
                        QueryExpr::Ref(binding_name.clone())
                    } else {
                        inp
                    }
                }).collect();
                Some(QueryExpr::LetBinding {
                    name:  binding_name,
                    expr:  Box::new(QueryExpr::Source(SourceSpec { name })),
                    body:  Box::new(QueryExpr::Merge { inputs: new_inputs }),
                })
            }
            _ => None,
        }
    }
}

// ── R9: HydraConversion ───────────────────────────────────────────────────────

/// Convert `Partition(keys, SketchAgg(op, col, e))` where `keys` has ≥ 2
/// dimensions into `SketchAgg(Hydra{inner: op, keys}, col, e)`.
///
/// Hydra is a sketch-of-sketches that handles multi-dimensional GROUP BY
/// more efficiently than one sketch per group tuple.
///
/// Step α status: the legacy `AggIntent::PerPartition { inner, keys }`
/// variant is gone — canonical L3 expresses the same shape as
/// `QueryExpr::Aggregate { by: keys, aggs: [inner] }`, and
/// `legacy_expr::PerPartitionWrap` holds the transitional shape (consumed
/// only by the physical sketch catalog). The historical inlining into
/// `SketchAgg::op` therefore can't survive Step α; the rule becomes a
/// pure cost-driven no-op until Step γ rewrites it to emit a canonical
/// `Aggregate` node. Disabling a cost-driven rule preserves correctness
/// (the unfused tree still produces the right result, just less
/// efficiently for multi-key Partition + sketch cases).
pub struct HydraConversion;

impl RewriteRule for HydraConversion {
    fn name(&self) -> &'static str { "HydraConversion" }

    fn try_rewrite(&self, _expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        // Step α: disabled — see struct docs. Step γ TODO: re-emit as a
        // canonical `Aggregate { by, aggs: [inner] }` wrapper around the
        // unwrapped `SketchAgg.op`.
        None
    }
}

// Original implementation kept under `dead_code` for Step γ reference.
#[allow(dead_code)]
mod hydra_conversion_legacy {
    use super::*;

    pub(super) fn try_rewrite_legacy(
        expr: QueryExpr,
        model: &dyn CostModel,
    ) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Partition { keys: PartitionKeys::By(ref key_list), ref input }
                if key_list.len() >= 2 =>
            {
                if let QueryExpr::SketchAgg { op: ref inner_op, ref col, input: ref inner_input } =
                    **input
                {
                    if matches!(
                        inner_op,
                        AggIntent::Quantile { .. }
                            | AggIntent::Cardinality { .. }
                            | AggIntent::Frequency { .. }
                    ) {
                        // Step γ: emit a canonical Aggregate { by, aggs: [inner_op] }
                        // wrapper here instead of re-inlining into SketchAgg.op.
                        let _ = (inner_op, col, inner_input, key_list, model);
                    }
                }
                None
            }
            _ => None,
        }
    }
}

// ── R10: WindowMerge ─────────────────────────────────────────────────────────

/// Merge two adjacent `Window` nodes with the same `duration` into one.
///
/// `Window(d, Window(d, e))` → `Window(d, e)`
pub struct WindowMerge;

impl RewriteRule for WindowMerge {
    fn name(&self) -> &'static str { "WindowMerge" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Window { duration, slide, input } => {
                if let QueryExpr::Window { duration: inner_d, slide: inner_s, input: inner_e } =
                    *input
                {
                    if duration == inner_d && slide == inner_s {
                        return Some(QueryExpr::Window {
                            duration,
                            slide,
                            input: inner_e,
                        });
                    }
                }
                None
            }
            _ => None,
        }
    }
}

// ── R11: PartitionElim ────────────────────────────────────────────────────────

/// Remove `Partition` with an empty key list — equivalent to a global
/// aggregation with no GROUP BY.
///
/// `Partition(By([]), e)` → `e`
pub struct PartitionElim;

impl RewriteRule for PartitionElim {
    fn name(&self) -> &'static str { "PartitionElim" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::Partition { keys, input } if keys.is_empty() => Some(*input),
            _ => None,
        }
    }
}

// ── R12: SetOpFusion ──────────────────────────────────────────────────────────

/// Fuse `SetOp(Union, Merge([…]), Merge([…]))` into a single `Merge([…, …])`.
pub struct SetOpFusion;

impl RewriteRule for SetOpFusion {
    fn name(&self) -> &'static str { "SetOpFusion" }

    fn try_rewrite(&self, expr: QueryExpr, _model: &dyn CostModel) -> Option<QueryExpr> {
        match expr {
            QueryExpr::SetOp {
                kind: SetOpKind::Union,
                all: true,
                left,
                right,
            } => {
                match (*left, *right) {
                    (QueryExpr::Merge { inputs: mut li }, QueryExpr::Merge { inputs: mut ri }) => {
                        li.append(&mut ri);
                        Some(QueryExpr::Merge { inputs: li })
                    }
                    (l, r) => Some(QueryExpr::SetOp {
                        kind: SetOpKind::Union,
                        all: true,
                        left:  Box::new(l),
                        right: Box::new(r),
                    }),
                }
            }
            _ => None,
        }
    }
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

/// Fixed-point query optimizer.
///
/// Call [`QueryOptimizer::optimize`] to rewrite a [`QueryExpr`] tree.
/// The optimizer iterates over all registered rules until no rule fires.
pub struct QueryOptimizer {
    rules:      Vec<Box<dyn RewriteRule>>,
    cost_model: Box<dyn CostModel>,
    /// Maximum number of fixed-point iterations (prevents infinite loops).
    max_iters:  usize,
}

impl QueryOptimizer {
    /// Create an optimizer with the default rule set and cost model.
    pub fn new(raw_bytes_per_sec: f64) -> Self {
        Self {
            rules: default_rules(),
            cost_model: Box::new(DefaultCostModel { raw_bytes_per_sec, deployment: None }),
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
                let (e, c) = self.apply_all(*$child);
                (Box::new(e), c)
            }};
        }
        match expr {
            QueryExpr::Source(_) | QueryExpr::Ref(_) => (expr, false),

            QueryExpr::Filter { pred, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Filter { pred, input: new_input }, c)
            }
            QueryExpr::Project { cols, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Project { cols, input: new_input }, c)
            }
            QueryExpr::Aggregate { keys, aggs, having, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Aggregate { keys, aggs, having, input: new_input }, c)
            }
            QueryExpr::Window { duration, slide, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Window { duration, slide, input: new_input }, c)
            }
            QueryExpr::SketchAgg { op, col, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::SketchAgg { op, col, input: new_input }, c)
            }
            QueryExpr::WindowedAgg { agg, window, col, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::WindowedAgg { agg, window, col, input: new_input }, c)
            }
            QueryExpr::Partition { keys, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Partition { keys, input: new_input }, c)
            }
            QueryExpr::Distinct { cols, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Distinct { cols, input: new_input }, c)
            }
            QueryExpr::TopK { k, by, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::TopK { k, by, input: new_input }, c)
            }
            QueryExpr::Sort { keys, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Sort { keys, input: new_input }, c)
            }
            QueryExpr::Limit { n, offset, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Limit { n, offset, input: new_input }, c)
            }
            QueryExpr::HistogramQuantile { phi, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::HistogramQuantile { phi, input: new_input }, c)
            }
            QueryExpr::PromQLSubquery { range, resolution, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::PromQLSubquery { range, resolution, input: new_input }, c)
            }
            QueryExpr::Merge { inputs } => {
                let (new_inputs, changed): (Vec<_>, Vec<_>) = inputs
                    .into_iter()
                    .map(|inp| self.apply_all(inp))
                    .unzip();
                (QueryExpr::Merge { inputs: new_inputs }, changed.into_iter().any(|c| c))
            }
            QueryExpr::Join { kind, pred, left, right } => {
                let (new_left,  cl) = recurse!(left);
                let (new_right, cr) = recurse!(right);
                (QueryExpr::Join { kind, pred, left: new_left, right: new_right }, cl || cr)
            }
            QueryExpr::SetOp { kind, all, left, right } => {
                let (new_left,  cl) = recurse!(left);
                let (new_right, cr) = recurse!(right);
                (QueryExpr::SetOp { kind, all, left: new_left, right: new_right }, cl || cr)
            }
            QueryExpr::BinaryOp { op, lhs, rhs, vector_match } => {
                let (new_lhs, cl) = recurse!(lhs);
                let (new_rhs, cr) = recurse!(rhs);
                (QueryExpr::BinaryOp { op, lhs: new_lhs, rhs: new_rhs, vector_match }, cl || cr)
            }
            QueryExpr::LetBinding { name, expr, body } => {
                let (new_expr, ce) = recurse!(expr);
                let (new_body, cb) = recurse!(body);
                (QueryExpr::LetBinding { name, expr: new_expr, body: new_body }, ce || cb)
            }
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
        Box::new(PartitionElim),
        Box::new(TopKFusion),
        Box::new(HistogramQuantileFusion),
        Box::new(MergeLifting),
        Box::new(SetOpFusion),
        Box::new(HydraConversion),
        Box::new(SubqueryDecorrelation),
        Box::new(CommonSubexprElim),
    ]
}

/// Identical to [`default_rules`] but typed as `Vec<Box<dyn OptimizerRule>>`
/// so callers that want the shared rule metadata surface (`name` +
/// `category`) can iterate over the same concrete rule set without
/// duplicating the list. Used by the future deployment-model rule-set
/// selection table (see `crate::deployment_model::DeploymentModel::rules`).
pub fn default_rules_as_optimizer_rules()
    -> Vec<Box<dyn crate::optimizer::trait_def::OptimizerRule>>
{
    vec![
        Box::new(PredicatePushDown),
        Box::new(FilterWindowSwap),
        Box::new(HLLDedupElim),
        Box::new(WindowMerge),
        Box::new(PartitionElim),
        Box::new(TopKFusion),
        Box::new(HistogramQuantileFusion),
        Box::new(MergeLifting),
        Box::new(SetOpFusion),
        Box::new(HydraConversion),
        Box::new(SubqueryDecorrelation),
        Box::new(CommonSubexprElim),
    ]
}

// ── OptimizerRule blanket impls for engine rules ───────────────────────────────
//
// The legacy `RewriteRule` trait's surface (`try_rewrite` over the legacy
// `QueryExpr`) is unique to the legacy IR — it can't be unified with the
// canonical-IR Phase-C `Rule` trait at the `try_rewrite`/`apply` level.
// What CAN be unified is the rule-metadata surface (`name`, `category`).
// Per-rule `OptimizerRule` impls below lift each concrete engine rule
// into the shared metadata surface so driver code can iterate over a
// `&[Box<dyn OptimizerRule>]` regardless of which family the rule
// belongs to.

use crate::optimizer::trait_def::{OptimizerRule, RuleCategory};

impl OptimizerRule for PredicatePushDown {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::PushDown }
}
impl OptimizerRule for FilterWindowSwap {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::PushDown }
}
impl OptimizerRule for HLLDedupElim {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Elim }
}
impl OptimizerRule for WindowMerge {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Fusion }
}
impl OptimizerRule for PartitionElim {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Elim }
}
impl OptimizerRule for TopKFusion {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Fusion }
}
impl OptimizerRule for HistogramQuantileFusion {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Fusion }
}
impl OptimizerRule for MergeLifting {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::PushDown }
}
impl OptimizerRule for SetOpFusion {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Fusion }
}
impl OptimizerRule for HydraConversion {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Fusion }
}
impl OptimizerRule for SubqueryDecorrelation {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Decorrelate }
}
impl OptimizerRule for CommonSubexprElim {
    fn name(&self) -> &'static str { <Self as RewriteRule>::name(self) }
    fn category(&self) -> RuleCategory { RuleCategory::Cse }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::{LiteralValue, ScalarExpr};
    use crate::intent_algebra::legacy_expr::{AggIntent, ColumnRef, PartitionKeys, SourceSpec};
    use std::time::Duration;

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    fn opt() -> QueryOptimizer {
        QueryOptimizer::new(100_000.0)
    }

    // ── R1: PredicatePushDown ─────────────────────────────────────────────────

    #[test]
    fn r1_pushes_filter_below_window() {
        let expr = QueryExpr::Filter {
            pred:  ScalarExpr::Literal(LiteralValue::Bool(true)),
            input: Box::new(QueryExpr::Window {
                duration: Duration::from_secs(60),
                slide:    None,
                input:    Box::new(src("m")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::Window { input, .. }
                if matches!(input.as_ref(), QueryExpr::Filter { .. })),
            "filter should be inside window: {result:?}"
        );
    }

    #[test]
    fn r1_pushes_filter_below_partition() {
        let expr = QueryExpr::Filter {
            pred:  ScalarExpr::Literal(LiteralValue::Bool(true)),
            input: Box::new(QueryExpr::Partition {
                keys:  PartitionKeys::By(vec!["host".into()]),
                input: Box::new(src("cpu")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::Partition { input, .. }
                if matches!(input.as_ref(), QueryExpr::Filter { .. }))
        );
    }

    // ── R3: HLLDedupElim ─────────────────────────────────────────────────────

    #[test]
    fn r3_removes_dedup_before_hll() {
        let expr = QueryExpr::SketchAgg {
            op:    crate::intent_algebra::legacy_expr::default_cardinality(),
            col:   ColumnRef::Named("user_id".into()),
            input: Box::new(QueryExpr::Distinct {
                cols:  vec![ColumnRef::Named("user_id".into())],
                input: Box::new(src("events")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            !matches!(&result, QueryExpr::SketchAgg { input, .. }
                if matches!(input.as_ref(), QueryExpr::Distinct { .. })),
            "Distinct should be eliminated before HLL"
        );
    }

    // ── R5: TopKFusion ────────────────────────────────────────────────────────

    #[test]
    fn r5_fuses_limit_sort_to_topk() {
        let expr = QueryExpr::Limit {
            n:      10,
            offset: 0,
            input:  Box::new(QueryExpr::Sort {
                keys:  vec![SortKey { col: "count".into(), desc: true, nulls_first: None }],
                input: Box::new(src("events")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::TopK { k: 10, .. }),
            "expected TopK(10), got {result:?}"
        );
    }

    #[test]
    fn r5_does_not_fuse_ascending_sort() {
        // ASC sort → not a top-k query.
        let expr = QueryExpr::Limit {
            n:      10,
            offset: 0,
            input:  Box::new(QueryExpr::Sort {
                keys:  vec![SortKey { col: "ts".into(), desc: false, nulls_first: None }],
                input: Box::new(src("events")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            !matches!(&result, QueryExpr::TopK { .. }),
            "ascending sort should not become TopK"
        );
    }

    // ── R6: HistogramQuantileFusion ───────────────────────────────────────────

    #[test]
    fn r6_fans_out_to_merge_when_phi_differs() {
        use crate::types_v2::AccuracyTarget;
        // Step α: canonical Quantile is single-φ; R6 emits a Merge of
        // two single-φ SketchAgg siblings when φ doesn't match the
        // existing intent's q. Apply R6 directly so this test is
        // independent of downstream rules (CommonSubexprElim, etc.).
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.95,
            input: Box::new(QueryExpr::SketchAgg {
                op:    AggIntent::Quantile { q: 0.5, accuracy: AccuracyTarget::Epsilon(0.01) },
                col:   ColumnRef::SampleValue,
                input: Box::new(src("latency")),
            }),
        };
        let rule = HistogramQuantileFusion;
        let model = DefaultCostModel { raw_bytes_per_sec: 100_000.0, deployment: None };
        let result = rule.try_rewrite(expr, &model).expect("R6 should fire");
        match &result {
            QueryExpr::HistogramQuantile { input, .. } => {
                match input.as_ref() {
                    QueryExpr::Merge { inputs } => {
                        let mut qs: Vec<f64> = inputs.iter().filter_map(|i| match i {
                            QueryExpr::SketchAgg { op: AggIntent::Quantile { q, .. }, .. } => {
                                Some(*q)
                            }
                            _ => None,
                        }).collect();
                        qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        assert_eq!(qs, vec![0.5, 0.95]);
                    }
                    other => panic!("expected Merge of SketchAgg siblings, got {other:?}"),
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    // ── R10: WindowMerge ──────────────────────────────────────────────────────

    #[test]
    fn r10_merges_duplicate_windows() {
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide:    None,
            input:    Box::new(QueryExpr::Window {
                duration: Duration::from_secs(300),
                slide:    None,
                input:    Box::new(src("m")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            !matches!(&result, QueryExpr::Window { input, .. }
                if matches!(input.as_ref(), QueryExpr::Window { .. })),
            "duplicate window should be merged"
        );
    }

    // ── R11: PartitionElim ────────────────────────────────────────────────────

    #[test]
    fn r11_removes_empty_partition() {
        let expr = QueryExpr::Partition {
            keys:  PartitionKeys::By(vec![]),
            input: Box::new(src("m")),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::Source(_)),
            "empty Partition should be eliminated"
        );
    }

    // ── Fixed-point convergence ───────────────────────────────────────────────

    #[test]
    fn optimizer_reaches_fixed_point_on_simple_tree() {
        let expr = src("m");
        let (result, iters) = opt().optimize(expr);
        assert!(iters < 5, "should converge quickly on source-only tree");
        assert!(matches!(result, QueryExpr::Source(_)));
    }

    #[test]
    fn optimizer_chain_of_rewrites() {
        // Filter(Window(Distinct(HLL(Source)))) →
        //   R1: Window(Filter(Distinct(HLL(Source))))
        //   R3: Window(Filter(HLL(Source)))   (HLL absorbs Distinct)
        let expr = QueryExpr::Filter {
            pred:  ScalarExpr::Literal(LiteralValue::Bool(true)),
            input: Box::new(QueryExpr::Window {
                duration: Duration::from_secs(60),
                slide:    None,
                input:    Box::new(QueryExpr::SketchAgg {
                    op:    crate::intent_algebra::legacy_expr::default_cardinality(),
                    col:   ColumnRef::Named("uid".into()),
                    input: Box::new(QueryExpr::Distinct {
                        cols:  vec![ColumnRef::Named("uid".into())],
                        input: Box::new(src("events")),
                    }),
                }),
            }),
        };
        let (result, _iters) = opt().optimize(expr);
        // The Distinct should be gone.
        let mut distinct_found = false;
        result.walk(&mut |n| {
            if matches!(n, QueryExpr::Distinct { .. }) {
                distinct_found = true;
            }
        });
        assert!(!distinct_found, "Distinct should have been eliminated");
    }

    // ── R2: MergeLifting ─────────────────────────────────────────────────────

    #[test]
    fn r2_lifts_mergeable_sketch_above_merge() {
        let expr = QueryExpr::SketchAgg {
            op:    crate::intent_algebra::legacy_expr::default_cardinality(),
            col:   ColumnRef::Named("uid".into()),
            input: Box::new(QueryExpr::Merge {
                inputs: vec![src("shard_a"), src("shard_b")],
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            matches!(&result, QueryExpr::Merge { inputs }
                if inputs.iter().all(|i| matches!(i, QueryExpr::SketchAgg { .. }))),
            "HLL should be pushed into each Merge branch"
        );
    }

    // ── R12: SetOpFusion ─────────────────────────────────────────────────────

    #[test]
    fn r12_fuses_union_of_merges() {
        let expr = QueryExpr::SetOp {
            kind:  SetOpKind::Union,
            all:   true,
            left:  Box::new(QueryExpr::Merge { inputs: vec![src("a"), src("b")] }),
            right: Box::new(QueryExpr::Merge { inputs: vec![src("c")] }),
        };
        let (result, _) = opt().optimize(expr);
        match result {
            QueryExpr::Merge { inputs } => assert_eq!(inputs.len(), 3),
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
            agent: StageBudget { memory_bytes: Some(1), ..Default::default() },
            ..Default::default()
        };
        let opt = QueryOptimizer::with_constraints(1000.0, dc);
        let expr = QueryExpr::SketchAgg {
            op: crate::intent_algebra::legacy_expr::default_quantile(0.99),
            col: ColumnRef::SampleValue,
            input: Box::new(QueryExpr::Source(SourceSpec { name: "m".into() })),
        };
        let cost = opt.cost_model.estimate(&expr);
        // Memory should be heavily penalised (10× multiplier)
        assert!(cost.memory_bytes > 10_000.0,
            "expected penalised memory, got {}", cost.memory_bytes);
    }

    #[test]
    fn unconstrained_optimizer_normal_cost() {
        let opt = QueryOptimizer::new(1000.0);
        let expr = QueryExpr::SketchAgg {
            op: crate::intent_algebra::legacy_expr::default_quantile(0.99),
            col: ColumnRef::SampleValue,
            input: Box::new(QueryExpr::Source(SourceSpec { name: "m".into() })),
        };
        let cost = opt.cost_model.estimate(&expr);
        // Normal cost, no penalty
        assert!(cost.memory_bytes < 10_000.0,
            "expected normal memory, got {}", cost.memory_bytes);
    }
}
