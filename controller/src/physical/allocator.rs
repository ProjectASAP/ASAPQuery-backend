//! Sketch allocator — converts a [`QueryExpr`] tree into an annotated
//! [`PlanNode`] tree, assigning every operator to a [`PipelineStage`] and
//! choosing between sketch and exact execution.
//!
//! # Algorithm
//!
//! 1. Walk the tree bottom-up (children before parents).
//! 2. For each node, determine the *preferred* stage using the rules below.
//! 3. If the preferred stage exceeds its resource budget, demote to the next
//!    stage in the chain: `Agent → Backend → Precompute → Db`.
//! 4. Annotate the node with the chosen sketch type, delta-encoding flag,
//!    and a human-readable rationale.
//!
//! ## Stage assignment rules
//!
//! | Node type | Default stage | Condition |
//! |-----------|---------------|-----------|
//! | Source, Filter, Window, Partition, Distinct | Agent | Always |
//! | SketchAgg (sketachable op, mergeable) | Agent | budget OK |
//! | SketchAgg (sketchable, mergeable) | Backend | agent budget exceeded |
//! | SketchAgg (sketchable, not mergeable: Avg) | Db | always |
//! | SketchAgg (exact: Sum/Count/Min/Max) | Backend | mergeable |
//! | TopK | Precompute | always |
//! | Merge | Backend | always |
//! | Aggregate, Project, Sort, Limit | Db | always |
//! | HistogramQuantile | Db | always |
//! | PromQLSubquery, BinaryOp | Precompute | has sketch children |
//! | LetBinding | same as body | propagated |

use crate::intent_algebra::legacy_expr::QueryExpr;
use super::plan::{
    CostEstimate, ExecutionMode, NodeAnnotation, PipelineStage, PlanNode,
};
use crate::intent_algebra::legacy_expr::{agg_is_exact, AggIntent};
use crate::intent_algebra::{infer_schema_for_root, Schema};
use crate::types::{SketchType, StageResourceBudgets};

// ── Resource budget tracker ───────────────────────────────────────────────────

/// Mutable budget state, consumed during allocation.
#[derive(Debug, Clone)]
struct BudgetState {
    agent_memory_remaining_bytes:   f64,
    backend_memory_remaining_bytes: f64,
}

impl BudgetState {
    fn from_budgets(b: &StageResourceBudgets) -> Self {
        Self {
            agent_memory_remaining_bytes:   b.agent_memory_bytes
                .map(|v| v as f64)
                .unwrap_or(f64::INFINITY),
            backend_memory_remaining_bytes: b.backend_memory_bytes
                .map(|v| v as f64)
                .unwrap_or(f64::INFINITY),
        }
    }

    fn fits_agent(&self, bytes: f64) -> bool {
        bytes <= self.agent_memory_remaining_bytes
    }

    fn fits_backend(&self, bytes: f64) -> bool {
        bytes <= self.backend_memory_remaining_bytes
    }

    fn consume_agent(&mut self, bytes: f64) {
        self.agent_memory_remaining_bytes =
            (self.agent_memory_remaining_bytes - bytes).max(0.0);
    }

    fn consume_backend(&mut self, bytes: f64) {
        self.backend_memory_remaining_bytes =
            (self.backend_memory_remaining_bytes - bytes).max(0.0);
    }
}

// ── Public allocator ──────────────────────────────────────────────────────────

/// Converts a (pre-optimised) [`QueryExpr`] tree into an annotated
/// [`PlanNode`] tree.
pub struct SketchAllocator {
    budgets:         StageResourceBudgets,
    raw_bytes_per_sec: f64,
}

impl SketchAllocator {
    /// Create an allocator.
    ///
    /// * `budgets` — per-stage memory caps (from [`StageResourceBudgets`]).
    /// * `raw_bytes_per_sec` — baseline bandwidth of the raw OTLP stream,
    ///   used to estimate compression ratios.
    pub fn new(budgets: StageResourceBudgets, raw_bytes_per_sec: f64) -> Self {
        Self { budgets, raw_bytes_per_sec }
    }

    /// Allocate stages for the entire expression tree.
    ///
    /// Step β: derives the root-level [`Schema`] from the outermost
    /// `Source` leaf (via [`infer_schema_for_root`]) and threads it
    /// through every recursive [`Self::alloc_node`] call. The allocator's
    /// stage-assignment logic is purely structural today, but the schema
    /// parameter is in place for Step γ when sketch-placement heuristics
    /// start consulting column types.
    pub fn allocate(&self, expr: QueryExpr) -> PlanNode {
        let schema = infer_schema_for_root(&expr);
        let mut budget = BudgetState::from_budgets(&self.budgets);
        self.alloc_node(expr, &mut budget, &schema)
    }

    // ── Recursive allocation ──────────────────────────────────────────────────

    fn alloc_node(
        &self,
        expr: QueryExpr,
        budget: &mut BudgetState,
        parent_schema: &Schema,
    ) -> PlanNode {
        match expr {
            // ── Leaves ───────────────────────────────────────────────────────
            QueryExpr::Source(_) | QueryExpr::Ref(_) => PlanNode::leaf(
                expr,
                PipelineStage::Agent,
                ExecutionMode::Passthrough,
            ),

            // ── Structural / filter nodes — always Agent ──────────────────
            QueryExpr::Filter { pred, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                let stage = PipelineStage::Agent;
                PlanNode {
                    expr: QueryExpr::Filter { pred, input: Box::new(child.expr.clone()) },
                    stage,
                    mode: ExecutionMode::Passthrough,
                    cost: CostEstimate {
                        bytes_per_sec: self.raw_bytes_per_sec * 0.5,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        rationale: "Filter pushed to Agent to reduce data volume early".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Window { duration, slide, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Window {
                        duration, slide,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Agent,
                    mode:  ExecutionMode::Passthrough,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Time window computed at Agent".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Partition { keys, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Partition {
                        keys,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Agent,
                    mode:  ExecutionMode::Passthrough,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Partition for GROUP BY at Agent".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Distinct { cols, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Distinct {
                        cols,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Agent,
                    mode:  ExecutionMode::Passthrough,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Distinct at Agent before sketch build".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            // ── Sketch aggregation — core allocation logic ────────────────
            QueryExpr::SketchAgg { op, col, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                self.alloc_sketch_agg(op, col, child, budget, parent_schema)
            }

            // ── WindowedAgg — treat as SketchAgg (window is informational) ──
            QueryExpr::WindowedAgg { agg, window: _, col, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                self.alloc_sketch_agg(agg, col, child, budget, parent_schema)
            }

            // ── TopK — Precompute engine ──────────────────────────────────
            QueryExpr::TopK { k, by, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::TopK {
                        k, by,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Precompute,
                    mode:  ExecutionMode::Sketch,
                    cost:  CostEstimate {
                        bytes_per_sec:  self.raw_bytes_per_sec * 0.05,
                        memory_bytes:   (k as f64) * 64.0,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        sketch_type: Some(SketchType::CountSketch),
                        rationale:   "TopK assigned to Precompute engine (CountSketch)".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            // ── Merge — Backend ───────────────────────────────────────────
            QueryExpr::Merge { inputs } => {
                let children: Vec<PlanNode> = inputs
                    .into_iter()
                    .map(|inp| self.alloc_node(inp, budget, parent_schema))
                    .collect();
                let mem: f64 = children.iter().map(|c| c.cost.memory_bytes).sum();
                PlanNode {
                    expr: QueryExpr::Merge {
                        inputs: children.iter().map(|c| c.expr.clone()).collect(),
                    },
                    stage: PipelineStage::Backend,
                    mode:  ExecutionMode::Passthrough,
                    cost:  CostEstimate {
                        bytes_per_sec: self.raw_bytes_per_sec * 0.1,
                        memory_bytes:  mem,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        rationale: "Sketch merge at Backend".into(),
                        ..Default::default()
                    },
                    children,
                }
            }

            // ── Exact / relational — Db ───────────────────────────────────
            QueryExpr::Aggregate { keys, aggs, having, input } => {
                // Step γ1 demonstration: bridge the legacy Aggregate to
                // canonical-shape data and enrich the annotation rationale
                // with the canonical intent kind(s). The legacy emit shape
                // is unchanged — `expr` still carries the legacy variant
                // (approach (c) per the migration spec). The bridge fails
                // gracefully when keys don't resolve or HAVING uses an
                // E-deferred ScalarExpr variant; the legacy annotation
                // text is used as the fallback in that case.
                let bridge_annotation: String = match
                    crate::intent_algebra::bridge_aggregate_to_canonical(
                        &keys, &aggs, &having, parent_schema,
                    )
                {
                    Ok(b) => {
                        let kinds: Vec<&'static str> = b.aggs.iter()
                            .map(canonical_intent_kind_str)
                            .collect();
                        format!(
                            "General Aggregate at Db (exact); canonical \
                             intents: {:?}, group_by_cols: {}",
                            kinds, b.by.len()
                        )
                    }
                    Err(_e) => "General Aggregate at Db (exact)".into(),
                };

                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Aggregate {
                        keys, aggs, having,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode:  ExecutionMode::Exact,
                    cost:  CostEstimate {
                        bytes_per_sec: self.raw_bytes_per_sec,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        rationale: bridge_annotation,
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Project { cols, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Project {
                        cols,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode:  ExecutionMode::Exact,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Project at Db".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Sort { keys, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Sort {
                        keys,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode:  ExecutionMode::Exact,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Sort at Db".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Limit { n, offset, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Limit {
                        n, offset,
                        input: Box::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode:  ExecutionMode::Exact,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Limit at Db".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Join { kind, pred, left, right } => {
                let left_node  = self.alloc_node(*left, budget, parent_schema);
                let right_node = self.alloc_node(*right, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::Join {
                        kind, pred,
                        left:  Box::new(left_node.expr.clone()),
                        right: Box::new(right_node.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode:  ExecutionMode::Exact,
                    cost:  CostEstimate {
                        bytes_per_sec: self.raw_bytes_per_sec,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        rationale: "Join at Db (exact)".into(),
                        ..Default::default()
                    },
                    children: vec![left_node, right_node],
                }
            }

            QueryExpr::SetOp { kind, all, left, right } => {
                let left_node  = self.alloc_node(*left, budget, parent_schema);
                let right_node = self.alloc_node(*right, budget, parent_schema);
                PlanNode {
                    expr: QueryExpr::SetOp {
                        kind, all,
                        left:  Box::new(left_node.expr.clone()),
                        right: Box::new(right_node.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode:  ExecutionMode::Exact,
                    cost:  CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "SetOp at Db".into(),
                        ..Default::default()
                    },
                    children: vec![left_node, right_node],
                }
            }

            // ── PromQL-specific ───────────────────────────────────────────
            QueryExpr::HistogramQuantile { phi, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                // If the child is a sketch, elevate to Precompute;
                // otherwise fall through to Db.
                let stage = if child.mode == ExecutionMode::Sketch {
                    PipelineStage::Precompute
                } else {
                    PipelineStage::Db
                };
                let rationale = format!("histogram_quantile(φ={phi}) at {stage}");
                PlanNode {
                    expr: QueryExpr::HistogramQuantile {
                        phi,
                        input: Box::new(child.expr.clone()),
                    },
                    stage,
                    mode:  ExecutionMode::Sketch,
                    cost:  CostEstimate {
                        bytes_per_sec:  self.raw_bytes_per_sec * 0.02,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        sketch_type: Some(SketchType::DDSketch),
                        rationale,
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::PromQLSubquery { range, resolution, input } => {
                let child = self.alloc_node(*input, budget, parent_schema);
                let stage = if child.mode == ExecutionMode::Sketch {
                    PipelineStage::Precompute
                } else {
                    PipelineStage::Db
                };
                let rationale = format!("PromQL subquery at {stage}");
                PlanNode {
                    expr: QueryExpr::PromQLSubquery {
                        range, resolution,
                        input: Box::new(child.expr.clone()),
                    },
                    stage,
                    mode: child.mode.clone(),
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale,
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::BinaryOp { op, lhs, rhs, vector_match } => {
                let left_node  = self.alloc_node(*lhs, budget, parent_schema);
                let right_node = self.alloc_node(*rhs, budget, parent_schema);
                let has_sketch = left_node.mode == ExecutionMode::Sketch
                    || right_node.mode == ExecutionMode::Sketch;
                let stage = if has_sketch {
                    PipelineStage::Precompute
                } else {
                    PipelineStage::Db
                };
                let rationale = format!("BinaryOp at {stage}");
                PlanNode {
                    expr: QueryExpr::BinaryOp {
                        op, vector_match,
                        lhs: Box::new(left_node.expr.clone()),
                        rhs: Box::new(right_node.expr.clone()),
                    },
                    stage,
                    mode: if has_sketch { ExecutionMode::Sketch } else { ExecutionMode::Exact },
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale,
                        ..Default::default()
                    },
                    children: vec![left_node, right_node],
                }
            }

            // ── Scoping constructs — propagate body's stage ───────────────
            QueryExpr::LetBinding { name, expr, body } => {
                let expr_node = self.alloc_node(*expr, budget, parent_schema);
                let body_node = self.alloc_node(*body, budget, parent_schema);
                let stage = body_node.stage.clone();
                let mode  = body_node.mode.clone();
                PlanNode {
                    expr: QueryExpr::LetBinding {
                        name,
                        expr: Box::new(expr_node.expr.clone()),
                        body: Box::new(body_node.expr.clone()),
                    },
                    stage,
                    mode,
                    cost:       CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "LetBinding: stage = body stage".into(),
                        ..Default::default()
                    },
                    children: vec![expr_node, body_node],
                }
            }
        }
    }

    // ── SketchAgg allocation (budget-driven demotion) ─────────────────────────

    fn alloc_sketch_agg(
        &self,
        op:     AggIntent,
        col:    crate::intent_algebra::legacy_expr::ColumnRef,
        child:  PlanNode,
        budget: &mut BudgetState,
        // Step β: schema in scope at this SketchAgg node. Unused today —
        // Step γ wires sketch-placement rules that consult column types
        // (e.g. KLL vs DDSketch for `value: Float64`).
        _parent_schema: &Schema,
    ) -> PlanNode {
        // Exact non-mergeable (Avg) → always Db.
        if matches!(&op, AggIntent::Avg) {
            return PlanNode {
                expr: QueryExpr::SketchAgg {
                    op,
                    col,
                    input: Box::new(child.expr.clone()),
                },
                stage: PipelineStage::Db,
                mode:  ExecutionMode::Exact,
                cost:  CostEstimate {
                    bytes_per_sec: self.raw_bytes_per_sec,
                    ..Default::default()
                },
                annotation: NodeAnnotation {
                    rationale: "Avg is not mergeable — must run at Db".into(),
                    ..Default::default()
                },
                children: vec![child],
            };
        }

        // Exact mergeable (Sum, Count, Min, Max) → Backend.
        if agg_is_exact(&op) {
            return PlanNode {
                expr: QueryExpr::SketchAgg {
                    op,
                    col,
                    input: Box::new(child.expr.clone()),
                },
                stage: PipelineStage::Backend,
                mode:  ExecutionMode::Exact,
                cost:  CostEstimate {
                    bytes_per_sec: self.raw_bytes_per_sec * 0.8,
                    ..Default::default()
                },
                annotation: NodeAnnotation {
                    rationale: "Exact(Sum/Count/Min/Max) merged at Backend".into(),
                    ..Default::default()
                },
                children: vec![child],
            };
        }

        // Sketch operators: resolve to physical, then try Agent → Backend → Precompute.
        let physical = super::planner::resolve(&op);
        let mem = estimated_sketch_memory(&op);
        let (sketch_type, params) = (physical.sketch_type, physical.sketch_params);

        if budget.fits_agent(mem) {
            budget.consume_agent(mem);
            return PlanNode {
                expr: QueryExpr::SketchAgg {
                    op,
                    col,
                    input: Box::new(child.expr.clone()),
                },
                stage: PipelineStage::Agent,
                mode:  ExecutionMode::Sketch,
                cost:  CostEstimate {
                    bytes_per_sec:  self.raw_bytes_per_sec * 0.05,
                    memory_bytes:   mem,
                    compression_ratio: 20.0,
                    ..Default::default()
                },
                annotation: NodeAnnotation {
                    sketch_type: Some(sketch_type),
                    sketch_params: Some(params),
                    rationale: "Sketch at Agent (within budget)".into(),
                    ..Default::default()
                },
                children: vec![child],
            };
        }

        if budget.fits_backend(mem) {
            budget.consume_backend(mem);
            return PlanNode {
                expr: QueryExpr::SketchAgg {
                    op,
                    col,
                    input: Box::new(child.expr.clone()),
                },
                stage: PipelineStage::Backend,
                mode:  ExecutionMode::Sketch,
                cost:  CostEstimate {
                    bytes_per_sec:  self.raw_bytes_per_sec * 0.1,
                    memory_bytes:   mem,
                    compression_ratio: 10.0,
                    ..Default::default()
                },
                annotation: NodeAnnotation {
                    sketch_type:     Some(sketch_type),
                    sketch_params:   Some(params),
                    rationale:       "Sketch demoted to Backend (Agent budget exceeded)".into(),
                    budget_demotion: true,
                    ..Default::default()
                },
                children: vec![child],
            };
        }

        // Both Agent and Backend budgets exceeded → Precompute.
        PlanNode {
            expr: QueryExpr::SketchAgg {
                op,
                col,
                input: Box::new(child.expr.clone()),
            },
            stage: PipelineStage::Precompute,
            mode:  ExecutionMode::Sketch,
            cost:  CostEstimate {
                bytes_per_sec:  self.raw_bytes_per_sec * 0.2,
                memory_bytes:   mem,
                compression_ratio: 5.0,
                ..Default::default()
            },
            annotation: NodeAnnotation {
                sketch_type:     Some(sketch_type),
                sketch_params:   Some(params),
                rationale:       "Sketch demoted to Precompute (Agent+Backend budgets exceeded)".into(),
                budget_demotion: true,
                ..Default::default()
            },
            children: vec![child],
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Estimate the memory footprint of a sketch in bytes.
fn estimated_sketch_memory(op: &AggIntent) -> f64 {
    super::sketch_catalog::estimated_sketch_memory_bytes(op) as f64
}

/// Map a canonical [`AggIntent`] to a short stable kind string for
/// annotation rationale text. Step γ1: used by the legacy
/// `QueryExpr::Aggregate` arm to enrich the rationale with the canonical
/// intent kinds reachable via `bridge_aggregate_to_canonical`.
fn canonical_intent_kind_str(intent: &AggIntent) -> &'static str {
    match intent {
        AggIntent::Count { .. } => "count",
        AggIntent::Sum => "sum",
        AggIntent::Min => "min",
        AggIntent::Max => "max",
        AggIntent::Avg => "avg",
        AggIntent::Quantile { .. } => "quantile",
        AggIntent::TopK { .. } => "topk",
        AggIntent::Cardinality { .. } => "cardinality",
        AggIntent::Frequency { .. } => "frequency",
        AggIntent::Rate { .. } => "rate",
        AggIntent::Increase { .. } => "increase",
        AggIntent::Absent => "absent",
        AggIntent::Present => "present",
        AggIntent::Delta { .. } => "delta",
        AggIntent::Deriv { .. } => "deriv",
        AggIntent::PredictLinear { .. } => "predict_linear",
        AggIntent::HoltWinters { .. } => "holt_winters",
        AggIntent::Idelta { .. } => "idelta",
        AggIntent::Irate { .. } => "irate",
        AggIntent::Resets { .. } => "resets",
        AggIntent::Changes { .. } => "changes",
    }
}

// sketch_type_for_op delegated to algebra::directory::sketch_type_and_params.

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::QueryExpr;
    use crate::physical::plan::{ExecutionMode, PipelineStage};
    use crate::intent_algebra::legacy_expr::{
        default_cardinality, default_frequency, default_quantile, AggIntent, ColumnRef,
        PartitionKeys, SourceSpec,
    };
    use crate::types::{SketchType, StageResourceBudgets};
    use std::time::Duration;

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    fn alloc(budgets: StageResourceBudgets, expr: QueryExpr) -> PlanNode {
        SketchAllocator::new(budgets, 100_000.0).allocate(expr)
    }

    fn unlimited() -> StageResourceBudgets {
        StageResourceBudgets::default()
    }

    fn tight_agent() -> StageResourceBudgets {
        StageResourceBudgets {
            agent_memory_bytes: Some(1),   // 1 byte — too small for any sketch
            ..Default::default()
        }
    }

    fn tight_all() -> StageResourceBudgets {
        StageResourceBudgets {
            agent_memory_bytes:   Some(1),
            backend_memory_bytes: Some(1),
            ..Default::default()
        }
    }

    // ── Source / leaf ─────────────────────────────────────────────────────────

    #[test]
    fn source_goes_to_agent() {
        let node = alloc(unlimited(), src("cpu"));
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.mode,  ExecutionMode::Passthrough);
    }

    // ── Filter ────────────────────────────────────────────────────────────────

    #[test]
    fn filter_at_agent() {
        use crate::intent_algebra::legacy_expr::{LiteralValue, ScalarExpr};
        let expr = QueryExpr::Filter {
            pred:  ScalarExpr::Literal(LiteralValue::Bool(true)),
            input: Box::new(src("m")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Agent);
    }

    // ── DDSketch within budget → Agent ────────────────────────────────────────

    #[test]
    fn ddsketch_within_budget_goes_to_agent() {
        let expr = QueryExpr::SketchAgg {
            op:    default_quantile(0.99),
            col:   ColumnRef::SampleValue,
            input: Box::new(src("latency")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.mode,  ExecutionMode::Sketch);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::DDSketch));
    }

    // ── DDSketch tight agent budget → Backend ─────────────────────────────────

    #[test]
    fn ddsketch_agent_budget_exceeded_goes_to_backend() {
        let expr = QueryExpr::SketchAgg {
            op:    default_quantile(0.99),
            col:   ColumnRef::SampleValue,
            input: Box::new(src("latency")),
        };
        let node = alloc(tight_agent(), expr);
        assert_eq!(node.stage, PipelineStage::Backend);
        assert!(node.annotation.budget_demotion);
    }

    // ── DDSketch tight agent+backend → Precompute ─────────────────────────────

    #[test]
    fn ddsketch_all_budgets_exceeded_goes_to_precompute() {
        let expr = QueryExpr::SketchAgg {
            op:    default_quantile(0.99),
            col:   ColumnRef::SampleValue,
            input: Box::new(src("latency")),
        };
        let node = alloc(tight_all(), expr);
        assert_eq!(node.stage, PipelineStage::Precompute);
        assert!(node.annotation.budget_demotion);
    }

    // ── Exact(Avg) → Db ───────────────────────────────────────────────────────

    #[test]
    fn exact_avg_goes_to_db() {
        let expr = QueryExpr::SketchAgg {
            op:    AggIntent::Avg,
            col:   ColumnRef::Named("price".into()),
            input: Box::new(src("trades")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Db);
        assert_eq!(node.mode,  ExecutionMode::Exact);
    }

    // ── Exact(Sum) → Backend ──────────────────────────────────────────────────

    #[test]
    fn exact_sum_goes_to_backend() {
        let expr = QueryExpr::SketchAgg {
            op:    AggIntent::Sum,
            col:   ColumnRef::Named("bytes".into()),
            input: Box::new(src("network")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Backend);
        assert_eq!(node.mode,  ExecutionMode::Exact);
    }

    // ── TopK → Precompute ─────────────────────────────────────────────────────

    #[test]
    fn topk_goes_to_precompute() {
        let expr = QueryExpr::TopK {
            k:     10,
            by:    vec!["symbol".into()],
            input: Box::new(src("trades")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Precompute);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::CountSketch));
    }

    // ── Merge → Backend ───────────────────────────────────────────────────────

    #[test]
    fn merge_goes_to_backend() {
        let expr = QueryExpr::Merge {
            inputs: vec![src("a"), src("b")],
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Backend);
    }

    // ── HLL → Agent ───────────────────────────────────────────────────────────

    #[test]
    fn hll_within_budget_at_agent() {
        let expr = QueryExpr::SketchAgg {
            op:    default_cardinality(),
            col:   ColumnRef::Named("uid".into()),
            input: Box::new(src("events")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::HLL));
    }

    // ── Frequency → Agent ─────────────────────────────────────────────────────

    #[test]
    fn frequency_within_budget_at_agent() {
        let expr = QueryExpr::SketchAgg {
            op:    default_frequency(),
            col:   ColumnRef::Wildcard,
            input: Box::new(src("requests")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::CountSketch));
    }

    // ── Join → Db ─────────────────────────────────────────────────────────────

    #[test]
    fn join_goes_to_db() {
        use crate::intent_algebra::legacy_expr::JoinKind;
        let expr = QueryExpr::Join {
            kind:  JoinKind::Inner,
            pred:  None,
            left:  Box::new(src("orders")),
            right: Box::new(src("items")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Db);
    }

    // ── HistogramQuantile + DDSketch → Precompute ─────────────────────────────

    #[test]
    fn histogram_quantile_over_sketch_at_precompute() {
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.95,
            input: Box::new(QueryExpr::SketchAgg {
                op:    default_quantile(0.95),
                col:   ColumnRef::SampleValue,
                input: Box::new(src("hist")),
            }),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Precompute);
    }

    // ── LetBinding inherits body stage ────────────────────────────────────────

    #[test]
    fn let_binding_inherits_body_stage() {
        let expr = QueryExpr::LetBinding {
            name: "base".into(),
            expr: Box::new(src("cpu")),
            body: Box::new(QueryExpr::TopK {
                k:     5,
                by:    vec![],
                input: Box::new(src("cpu")),
            }),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Precompute);
    }

    // ── Memory estimate helpers ───────────────────────────────────────────────

    #[test]
    fn cardinality_memory_estimate() {
        let mem = estimated_sketch_memory(&default_cardinality());
        assert!(mem > 0.0);
    }

    #[test]
    fn frequency_memory_estimate() {
        let op = default_frequency();
        let mem = estimated_sketch_memory(&op);
        assert!(mem > 0.0);
    }

    // ── PlanSummary from allocated tree ──────────────────────────────────────

    #[test]
    fn plan_summary_shows_bandwidth_saved() {
        let expr = QueryExpr::SketchAgg {
            op:    default_quantile(0.99),
            col:   ColumnRef::SampleValue,
            input: Box::new(src("latency")),
        };
        let node   = alloc(unlimited(), expr);
        let summary = node.summarise(100_000.0);
        // sketch reduces to ~5% → saved ~95 000 B/s
        assert!(summary.bandwidth_saved_bytes_per_sec > 50_000.0);
        assert!(summary.agent_memory_bytes > 0.0);
    }
}
