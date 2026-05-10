//! Cost-based fixed-point query optimizer.
//!
//! The optimizer applies a set of algebraic rewrite rules to a
//! [`QueryExpr`](super::expr::QueryExpr) tree until no rule fires (fixed
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

use super::expr::{QueryExpr, ScalarExpr, SetOpKind, SortKey};
use super::expr::{AggIntent, PartitionKeys, SourceSpec};

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

/// Performance and capability profile for a single sketch implementation.
///
/// Used by the optimizer to compare candidates and by the physical planner
/// to check whether a sketch fits within a stage's budget.
#[derive(Debug, Clone)]
pub struct SketchCapability {
    /// Insertion throughput (samples/sec at 1 core).
    pub insert_throughput: f64,
    /// Query throughput (queries/sec at 1 core).
    pub query_throughput: f64,
    /// Memory footprint per series (bytes).
    pub memory_bytes_per_series: u64,
    /// CPU cost per insert (µs/sample).
    pub cpu_micros_per_insert: f64,
    /// Transmission size per flush (bytes).
    pub transmission_bytes: u64,
    /// Which logical aggregation intents this sketch supports.
    pub supported_intents: Vec<SupportedIntent>,
    /// Whether the sketch supports merge (sketch(A∪B) = merge(sketch(A), sketch(B))).
    pub mergeable: bool,
    /// Whether the sketch supports delta encoding.
    pub supports_delta: bool,
    /// Whether the sketch supports sliding windows natively.
    pub supports_sliding_window: bool,
}

/// A logical aggregation intent that a sketch can serve.
#[derive(Debug, Clone, PartialEq)]
pub enum SupportedIntent {
    Quantile,
    Cardinality,
    Frequency,
    Extrema,
}

/// YAML-serializable capability profile (for loading from config).
#[derive(Debug, Clone, serde::Deserialize)]
struct SketchCapabilityYaml {
    insert_throughput: f64,
    query_throughput: f64,
    memory_bytes_per_series: u64,
    cpu_micros_per_insert: f64,
    transmission_bytes: u64,
    supported_intents: Vec<String>,
    mergeable: bool,
    supports_delta: bool,
    supports_sliding_window: bool,
}

impl SketchCapabilityYaml {
    fn to_capability(&self) -> SketchCapability {
        let intents = self.supported_intents.iter().filter_map(|s| match s.as_str() {
            "quantile" => Some(SupportedIntent::Quantile),
            "cardinality" => Some(SupportedIntent::Cardinality),
            "frequency" => Some(SupportedIntent::Frequency),
            "extrema" => Some(SupportedIntent::Extrema),
            _ => None,
        }).collect();
        SketchCapability {
            insert_throughput: self.insert_throughput,
            query_throughput: self.query_throughput,
            memory_bytes_per_series: self.memory_bytes_per_series,
            cpu_micros_per_insert: self.cpu_micros_per_insert,
            transmission_bytes: self.transmission_bytes,
            supported_intents: intents,
            mergeable: self.mergeable,
            supports_delta: self.supports_delta,
            supports_sliding_window: self.supports_sliding_window,
        }
    }
}

/// YAML file structure for all sketch capabilities.
#[derive(Debug, Clone, serde::Deserialize)]
struct SketchCapabilitiesFile {
    ddsketch: SketchCapabilityYaml,
    kll: SketchCapabilityYaml,
    hll: SketchCapabilityYaml,
    count_sketch: SketchCapabilityYaml,
    count_min_sketch: SketchCapabilityYaml,
}

/// Load sketch capabilities from a YAML file.
///
/// Falls back to built-in defaults if the file is missing or malformed.
pub fn load_sketch_capabilities(path: &str) -> std::collections::HashMap<crate::types::SketchType, SketchCapability> {
    use crate::types::SketchType;
    if let Ok(contents) = std::fs::read_to_string(path) {
        if let Ok(file) = serde_yaml::from_str::<SketchCapabilitiesFile>(&contents) {
            let mut map = std::collections::HashMap::new();
            map.insert(SketchType::DDSketch, file.ddsketch.to_capability());
            map.insert(SketchType::KLL, file.kll.to_capability());
            map.insert(SketchType::HLL, file.hll.to_capability());
            map.insert(SketchType::CountSketch, file.count_sketch.to_capability());
            map.insert(SketchType::CountMinSketch, file.count_min_sketch.to_capability());
            return map;
        }
    }
    // Fallback: built-in defaults.
    let mut map = std::collections::HashMap::new();
    for st in &[SketchType::DDSketch, SketchType::KLL, SketchType::HLL, SketchType::CountSketch, SketchType::CountMinSketch] {
        map.insert(st.clone(), sketch_capability(st));
    }
    map
}

/// Built-in capability profiles for known sketch types.
///
/// These are compiled-in defaults. For deployment-specific values, load from
/// `sketch_capabilities.yml` via [`load_sketch_capabilities`], or run benchmarks
/// with `e2esdkbench` and update the YAML.
pub fn sketch_capability(st: &crate::types::SketchType) -> SketchCapability {
    use crate::types::SketchType;
    match st {
        SketchType::DDSketch => SketchCapability {
            insert_throughput: 10_000_000.0,
            query_throughput: 50_000_000.0,
            memory_bytes_per_series: 4_096,
            cpu_micros_per_insert: 0.1,
            transmission_bytes: 4_096,
            supported_intents: vec![SupportedIntent::Quantile, SupportedIntent::Extrema],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
        SketchType::KLL => SketchCapability {
            insert_throughput: 5_000_000.0,
            query_throughput: 20_000_000.0,
            memory_bytes_per_series: 8_192,
            cpu_micros_per_insert: 0.2,
            transmission_bytes: 8_192,
            supported_intents: vec![SupportedIntent::Quantile, SupportedIntent::Extrema],
            mergeable: true,
            supports_delta: false,
            supports_sliding_window: false,
        },
        SketchType::HLL => SketchCapability {
            insert_throughput: 20_000_000.0,
            query_throughput: 100_000_000.0,
            memory_bytes_per_series: 16_384,
            cpu_micros_per_insert: 0.05,
            transmission_bytes: 16_384,
            supported_intents: vec![SupportedIntent::Cardinality],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
        SketchType::CountSketch => SketchCapability {
            insert_throughput: 8_000_000.0,
            query_throughput: 10_000_000.0,
            memory_bytes_per_series: 80_000,
            cpu_micros_per_insert: 0.5,
            transmission_bytes: 80_000,
            supported_intents: vec![SupportedIntent::Frequency],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
        SketchType::CountMinSketch => SketchCapability {
            insert_throughput: 8_000_000.0,
            query_throughput: 10_000_000.0,
            memory_bytes_per_series: 80_000,
            cpu_micros_per_insert: 0.5,
            transmission_bytes: 80_000,
            supported_intents: vec![SupportedIntent::Frequency],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    }
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
        // Sketch nodes reduce bandwidth; exact nodes pass through.
        let factor = match expr {
            QueryExpr::SketchAgg { op, .. } | QueryExpr::WindowedAgg { agg: op, .. } => match op {
                AggIntent::Quantile { .. }    => 0.05,
                AggIntent::Cardinality { .. } => 0.02,
                AggIntent::Frequency { .. }   => 0.03,
                AggIntent::Exact(_)           => 1.0,
                _                             => 0.1,
            },
            QueryExpr::Merge { inputs } => 1.0 / (inputs.len().max(1) as f64),
            QueryExpr::Filter { .. }    => 0.5,
            QueryExpr::TopK { k, .. }   => (*k as f64).recip().min(0.1),
            QueryExpr::Partition { .. }  => 0.8, // partition adds overhead
            QueryExpr::Dedup { .. }      => 0.9,
            _                           => 1.0,
        };

        let memory = match expr {
            QueryExpr::SketchAgg { op, .. } | QueryExpr::WindowedAgg { agg: op, .. } =>
                crate::algebra::directory::estimated_sketch_memory_bytes(op) as f64,
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
                | QueryExpr::Dedup { .. } => &dc.backend_collector,
                QueryExpr::TopK { .. } | QueryExpr::HistogramQuantile { .. }
                | QueryExpr::BinaryOp { .. } | QueryExpr::PromQLSubquery { .. } => &dc.backend_db,
                QueryExpr::Aggregate { .. } => &dc.original_db,
                _ => &dc.agent,
            };

            // For sketch nodes, check sketch capability against stage budget.
            if let QueryExpr::SketchAgg { op, .. } | QueryExpr::WindowedAgg { agg: op, .. } = expr {
                let sketch_type = crate::algebra::directory::sketch_type_for_op(op);
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
                    if matches!(window.kind, crate::algebra::expr::WindowKind::Sliding { .. })
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
                if op.is_mergeable() =>
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
                if let QueryExpr::Dedup { input: inner, .. } = *input {
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
        match expr {
            QueryExpr::HistogramQuantile { phi, input } => {
                match *input {
                    QueryExpr::SketchAgg {
                        op: AggIntent::Quantile { quantiles, accuracy },
                        col,
                        input: inner,
                    } => {
                        if !quantiles.contains(&phi) {
                            let mut new_qs = quantiles;
                            new_qs.push(phi);
                            new_qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
                            Some(QueryExpr::HistogramQuantile {
                                phi,
                                input: Box::new(QueryExpr::SketchAgg {
                                    op:    AggIntent::Quantile { quantiles: new_qs, accuracy },
                                    col,
                                    input: inner,
                                }),
                            })
                        } else {
                            Some(QueryExpr::HistogramQuantile {
                                phi,
                                input: Box::new(QueryExpr::SketchAgg {
                                    op: AggIntent::Quantile { quantiles, accuracy },
                                    col,
                                    input: inner,
                                }),
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
pub struct HydraConversion;

impl RewriteRule for HydraConversion {
    fn name(&self) -> &'static str { "HydraConversion" }

    fn try_rewrite(&self, expr: QueryExpr, model: &dyn CostModel) -> Option<QueryExpr> {
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
                        let hydra_op = AggIntent::PerPartition {
                            inner: Box::new(inner_op.clone()),
                            keys:  key_list.clone(),
                        };
                        let candidate = QueryExpr::SketchAgg {
                            op:    hydra_op,
                            col:   col.clone(),
                            input: inner_input.clone(),
                        };
                        let old_cost = model.estimate(&expr);
                        let new_cost = model.estimate(&candidate);
                        if new_cost.memory_bytes < old_cost.memory_bytes
                            || new_cost.bytes_per_sec < old_cost.bytes_per_sec
                        {
                            return Some(candidate);
                        }
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
            QueryExpr::Dedup { col, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::Dedup { col, input: new_input }, c)
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
            QueryExpr::WindowFunc { func, partition_by, order_by, frame, input } => {
                let (new_input, c) = recurse!(input);
                (QueryExpr::WindowFunc { func, partition_by, order_by, frame, input: new_input }, c)
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
            QueryExpr::JoinSketch { join_key, outer, inner } => {
                let (new_outer, co) = recurse!(outer);
                let (new_inner, ci) = recurse!(inner);
                (QueryExpr::JoinSketch { join_key, outer: new_outer, inner: new_inner }, co || ci)
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
            QueryExpr::Subquery { alias, expr } => {
                let (new_expr, c) = recurse!(expr);
                (QueryExpr::Subquery { alias, expr: new_expr }, c)
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::expr::{LiteralValue, ScalarExpr};
    use crate::algebra::expr::{AggIntent, ColumnRef, PartitionKeys, SourceSpec};
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
            op:    AggIntent::default_cardinality(),
            col:   ColumnRef::Named("user_id".into()),
            input: Box::new(QueryExpr::Dedup {
                col:   "user_id".into(),
                input: Box::new(src("events")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        assert!(
            !matches!(&result, QueryExpr::SketchAgg { input, .. }
                if matches!(input.as_ref(), QueryExpr::Dedup { .. })),
            "Dedup should be eliminated before HLL"
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
    fn r6_adds_phi_to_ddsketch_quantiles() {
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.95,
            input: Box::new(QueryExpr::SketchAgg {
                op:    AggIntent::Quantile { quantiles: vec![0.5], accuracy: 0.01 },
                col:   ColumnRef::SampleValue,
                input: Box::new(src("latency")),
            }),
        };
        let (result, _) = opt().optimize(expr);
        match &result {
            QueryExpr::HistogramQuantile { input, .. } => {
                if let QueryExpr::SketchAgg { op: AggIntent::Quantile { quantiles, .. }, .. } =
                    input.as_ref()
                {
                    assert!(quantiles.contains(&0.95), "0.95 should be in DDSketch quantiles");
                    assert!(quantiles.contains(&0.5), "0.5 should still be present");
                } else {
                    panic!("expected DDSketch under HistogramQuantile");
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
        // Filter(Window(Dedup(HLL(Source)))) →
        //   R1: Window(Filter(Dedup(HLL(Source))))
        //   R3: Window(Filter(HLL(Source)))   (HLL absorbs Dedup)
        let expr = QueryExpr::Filter {
            pred:  ScalarExpr::Literal(LiteralValue::Bool(true)),
            input: Box::new(QueryExpr::Window {
                duration: Duration::from_secs(60),
                slide:    None,
                input:    Box::new(QueryExpr::SketchAgg {
                    op:    AggIntent::default_cardinality(),
                    col:   ColumnRef::Named("uid".into()),
                    input: Box::new(QueryExpr::Dedup {
                        col:   "uid".into(),
                        input: Box::new(src("events")),
                    }),
                }),
            }),
        };
        let (result, _iters) = opt().optimize(expr);
        // The Dedup should be gone.
        let mut dedup_found = false;
        result.walk(&mut |n| {
            if matches!(n, QueryExpr::Dedup { .. }) {
                dedup_found = true;
            }
        });
        assert!(!dedup_found, "Dedup should have been eliminated");
    }

    // ── R2: MergeLifting ─────────────────────────────────────────────────────

    #[test]
    fn r2_lifts_mergeable_sketch_above_merge() {
        let expr = QueryExpr::SketchAgg {
            op:    AggIntent::default_cardinality(),
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
            op: AggIntent::default_quantile(vec![0.99]),
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
            op: AggIntent::default_quantile(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(QueryExpr::Source(SourceSpec { name: "m".into() })),
        };
        let cost = opt.cost_model.estimate(&expr);
        // Normal cost, no penalty
        assert!(cost.memory_bytes < 10_000.0,
            "expected normal memory, got {}", cost.memory_bytes);
    }
}
