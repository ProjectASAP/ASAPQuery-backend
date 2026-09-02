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
//! | Scan, Ref, Filter, Window, Partition, Distinct | Agent | Always |
//! | Aggregate (single sketch intent, mergeable) | Agent | budget OK |
//! | Aggregate (single sketch intent, mergeable) | Backend | agent budget exceeded |
//! | Aggregate (single intent: Avg) | Db | always (not mergeable) |
//! | Aggregate (single exact intent: Sum/Count/Min/Max) | Backend | mergeable |
//! | Aggregate (single TopK intent) | Precompute | always |
//! | Aggregate (multi-intent or HAVING) | Db | always (general exact) |
//! | Merge | Backend | always |
//! | Project, Sort, Limit, Join, SetOp | Db | always |
//! | Subquery, BinaryOp | Precompute | has sketch children |
//! | LetBinding | same as body | propagated |
//!
//! Step γ7: the canonical IR folds the legacy `SketchAgg` / `WindowedAgg`
//! / `TopK` variants all into `Aggregate`, so the single `Aggregate` arm
//! dispatches on shape. `histogram_quantile(φ, …)` was already
//! substituted at the parser level into a plain `Aggregate{Quantile(φ)}`
//! (Step γ5). A `Window` over a single-intent `Aggregate` is the
//! canonical fold of the legacy `WindowedAgg`; for *stage* allocation
//! the window is informational (the `Window` arm is a passthrough and
//! the inner `Aggregate` arm does the sketch placement). The
//! window-defines-sketch-lifecycle fusion that *does* matter is a
//! `physical::planner` concern — see `physical::window_fusion`.

use std::rc::Rc;

use super::plan::{CostEstimate, ExecutionMode, NodeAnnotation, PipelineStage, PlanNode};
use crate::types::{SketchType, StageResourceBudgets};
use planner_types::pre_asap::agg_is_exact;
use planner_types::pre_asap::AggIntent;
use planner_types::pre_asap::{QueryExpr, Reduction};

// ── Resource budget tracker ───────────────────────────────────────────────────

/// Mutable budget state, consumed during allocation.
#[derive(Debug, Clone)]
struct BudgetState {
    agent_memory_remaining_bytes: f64,
    backend_memory_remaining_bytes: f64,
}

impl BudgetState {
    fn from_budgets(b: &StageResourceBudgets) -> Self {
        Self {
            agent_memory_remaining_bytes: b
                .agent_memory_bytes
                .map(|v| v as f64)
                .unwrap_or(f64::INFINITY),
            backend_memory_remaining_bytes: b
                .backend_memory_bytes
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
        self.agent_memory_remaining_bytes = (self.agent_memory_remaining_bytes - bytes).max(0.0);
    }

    fn consume_backend(&mut self, bytes: f64) {
        self.backend_memory_remaining_bytes =
            (self.backend_memory_remaining_bytes - bytes).max(0.0);
    }
}

// ── Public allocator ──────────────────────────────────────────────────────────

/// Converts a (pre-optimised) canonical [`QueryExpr`] tree into an
/// annotated [`PlanNode`] tree.
pub struct SketchAllocator {
    budgets: StageResourceBudgets,
    raw_bytes_per_sec: f64,
}

impl SketchAllocator {
    /// Create an allocator.
    ///
    /// * `budgets` — per-stage memory caps (from [`StageResourceBudgets`]).
    /// * `raw_bytes_per_sec` — baseline bandwidth of the raw OTLP stream,
    ///   used to estimate compression ratios.
    pub fn new(budgets: StageResourceBudgets, raw_bytes_per_sec: f64) -> Self {
        Self {
            budgets,
            raw_bytes_per_sec,
        }
    }

    /// Allocate stages for the entire expression tree.
    ///
    /// The canonical `Aggregate.by` is already positional (`Vec<ColumnId>`),
    /// so — unlike the legacy allocator — no inherited `Schema` needs to be
    /// threaded for column resolution; stage assignment is purely structural.
    pub fn allocate(&self, expr: QueryExpr) -> PlanNode {
        let mut budget = BudgetState::from_budgets(&self.budgets);
        self.alloc_node(expr, &mut budget)
    }

    // ── Recursive allocation ──────────────────────────────────────────────────

    fn alloc_node(&self, expr: QueryExpr, budget: &mut BudgetState) -> PlanNode {
        match expr {
            // ── Leaves ───────────────────────────────────────────────────────
            QueryExpr::Scan { .. } => {
                PlanNode::leaf(expr, PipelineStage::Agent, ExecutionMode::Passthrough)
            }

            // ── Structural / filter nodes — always Agent ──────────────────
            QueryExpr::Filter { pred, child } => {
                let child = self.alloc_node((*child).clone(), budget);
                PlanNode {
                    expr: QueryExpr::Filter {
                        pred,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Agent,
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

            // A `TimeRange` over a single-intent `Aggregate` is the
            // canonical fold of the legacy `WindowedAgg` (was `Window`
            // before the ASAPPlanner pin migration); for *stage*
            // allocation the range is informational — this arm is a
            // passthrough and the inner `Aggregate` arm does the sketch
            // placement.
            QueryExpr::TimeRange { range, child } => {
                let child = self.alloc_node((*child).clone(), budget);
                PlanNode {
                    expr: QueryExpr::TimeRange {
                        range,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Agent,
                    mode: ExecutionMode::Passthrough,
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Time window computed at Agent".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Dedup { cols, child } => {
                let child = self.alloc_node((*child).clone(), budget);
                PlanNode {
                    expr: QueryExpr::Dedup {
                        cols,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Agent,
                    mode: ExecutionMode::Passthrough,
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Distinct at Agent before sketch build".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            // ── Aggregate — the canonical IR folds legacy SketchAgg /
            // WindowedAgg / TopK all into Aggregate, so dispatch on shape:
            //   * single TopK intent, no HAVING → heavy-hitter at Precompute
            //   * single other intent, no HAVING → budget-driven sketch
            //   * multi-intent or HAVING → general exact Aggregate at Db
            QueryExpr::Aggregate {
                reduction,
                measures: aggs,
                output_names,
                having,
                child,
            } => {
                if aggs.len() == 1 && having.is_none() {
                    if let AggIntent::TopK { k, .. } = &aggs[0] {
                        let k = *k;
                        let child = self.alloc_node((*child).clone(), budget);
                        return PlanNode {
                            expr: QueryExpr::Aggregate {
                                reduction,
                                measures: aggs,
                                output_names,
                                having,
                                child: Rc::new(child.expr.clone()),
                            },
                            stage: PipelineStage::Precompute,
                            mode: ExecutionMode::Sketch,
                            cost: CostEstimate {
                                bytes_per_sec: self.raw_bytes_per_sec * 0.05,
                                memory_bytes: (k as f64) * 64.0,
                                ..Default::default()
                            },
                            annotation: NodeAnnotation {
                                sketch_type: Some(SketchType::CountSketch),
                                rationale: format!(
                                    "TopK(k={k}) assigned to Precompute engine (CountSketch)"
                                ),
                                ..Default::default()
                            },
                            children: vec![child],
                        };
                    }
                    // Single non-TopK intent → budget-driven sketch agg.
                    let child = self.alloc_node((*child).clone(), budget);
                    return self.alloc_sketch_agg(reduction, aggs, output_names, child, budget);
                }
                // General multi-intent / HAVING aggregate → Db (exact).
                let child = self.alloc_node((*child).clone(), budget);
                let kinds: Vec<&'static str> = aggs.iter().map(canonical_intent_kind_str).collect();
                PlanNode {
                    expr: QueryExpr::Aggregate {
                        reduction,
                        measures: aggs,
                        output_names,
                        having,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode: ExecutionMode::Exact,
                    cost: CostEstimate {
                        bytes_per_sec: self.raw_bytes_per_sec,
                        ..Default::default()
                    },
                    annotation: NodeAnnotation {
                        rationale: format!("General Aggregate at Db (exact); intents: {kinds:?}"),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            // ── Merge — Backend ───────────────────────────────────────────
            QueryExpr::Concat { children: inputs } => {
                let children: Vec<PlanNode> = inputs
                    .into_iter()
                    .map(|inp| self.alloc_node(inp, budget))
                    .collect();
                let mem: f64 = children.iter().map(|c| c.cost.memory_bytes).sum();
                PlanNode {
                    expr: QueryExpr::Concat {
                        children: children.iter().map(|c| c.expr.clone()).collect(),
                    },
                    stage: PipelineStage::Backend,
                    mode: ExecutionMode::Passthrough,
                    cost: CostEstimate {
                        bytes_per_sec: self.raw_bytes_per_sec * 0.1,
                        memory_bytes: mem,
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
            QueryExpr::Project {
                cols,
                qualifier,
                child,
            } => {
                let child = self.alloc_node((*child).clone(), budget);
                PlanNode {
                    expr: QueryExpr::Project {
                        cols,
                        qualifier,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode: ExecutionMode::Exact,
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Project at Db".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Sort {
                keys,
                partition_by,
                child,
            } => {
                let child = self.alloc_node((*child).clone(), budget);
                PlanNode {
                    expr: QueryExpr::Sort {
                        keys,
                        partition_by,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode: ExecutionMode::Exact,
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Sort at Db".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Limit { n, offset, child } => {
                let child = self.alloc_node((*child).clone(), budget);
                PlanNode {
                    expr: QueryExpr::Limit {
                        n,
                        offset,
                        child: Rc::new(child.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode: ExecutionMode::Exact,
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "Limit at Db".into(),
                        ..Default::default()
                    },
                    children: vec![child],
                }
            }

            QueryExpr::Join {
                kind,
                pred,
                left,
                right,
            } => {
                let left_node = self.alloc_node((*left).clone(), budget);
                let right_node = self.alloc_node((*right).clone(), budget);
                PlanNode {
                    expr: QueryExpr::Join {
                        kind,
                        pred,
                        left: Rc::new(left_node.expr.clone()),
                        right: Rc::new(right_node.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode: ExecutionMode::Exact,
                    cost: CostEstimate {
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

            QueryExpr::SetOp {
                kind,
                all,
                left,
                right,
            } => {
                let left_node = self.alloc_node((*left).clone(), budget);
                let right_node = self.alloc_node((*right).clone(), budget);
                PlanNode {
                    expr: QueryExpr::SetOp {
                        kind,
                        all,
                        left: Rc::new(left_node.expr.clone()),
                        right: Rc::new(right_node.expr.clone()),
                    },
                    stage: PipelineStage::Db,
                    mode: ExecutionMode::Exact,
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale: "SetOp at Db".into(),
                        ..Default::default()
                    },
                    children: vec![left_node, right_node],
                }
            }

            // ── PromQL sub-query ──────────────────────────────────────────
            QueryExpr::PromqlSubquery {
                range,
                resolution,
                child,
            } => {
                let child = self.alloc_node((*child).clone(), budget);
                let stage = if child.mode == ExecutionMode::Sketch {
                    PipelineStage::Precompute
                } else {
                    PipelineStage::Db
                };
                let rationale = format!("PromQL subquery at {stage}");
                PlanNode {
                    expr: QueryExpr::PromqlSubquery {
                        range,
                        resolution,
                        child: Rc::new(child.expr.clone()),
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

            QueryExpr::BinaryOp {
                op,
                lhs,
                rhs,
                vector_match,
            } => {
                let left_node = self.alloc_node((*lhs).clone(), budget);
                let right_node = self.alloc_node((*rhs).clone(), budget);
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
                        op,
                        vector_match,
                        lhs: Rc::new(left_node.expr.clone()),
                        rhs: Rc::new(right_node.expr.clone()),
                    },
                    stage,
                    mode: if has_sketch {
                        ExecutionMode::Sketch
                    } else {
                        ExecutionMode::Exact
                    },
                    cost: CostEstimate::default(),
                    annotation: NodeAnnotation {
                        rationale,
                        ..Default::default()
                    },
                    children: vec![left_node, right_node],
                }
            }

            // `LetBinding`/`Ref` don't exist in the canonical `QueryExpr`
            // anymore (see `optimizer::engine::CommonSubexprElim`'s doc).

            // `asap_ir`'s PromQL-surface superset (Scalar / EvalTime /
            // VectorFromScalar / ScalarFromVector / Relabel / InfoJoin /
            // Sample / TimeRange / TimeShift / WindowFunc) — not yet
            // targeted by a dedicated allocation rule in this deployment.
            // Leaves stay informational Agent leaves; every other new
            // variant wraps exactly one child, recursed into and staged
            // as an Agent passthrough (mirroring `Filter`/`Window` above)
            // until a real rule is written for them.
            QueryExpr::PromqlScalarBridge(_) | QueryExpr::EvalTimestamp => {
                PlanNode::leaf(expr, PipelineStage::Agent, ExecutionMode::Passthrough)
            }
            QueryExpr::PromqlVectorFromScalar(child) => {
                self.alloc_passthrough_child(child, "VectorFromScalar", budget, |c| {
                    QueryExpr::PromqlVectorFromScalar(Rc::new(c))
                })
            }
            QueryExpr::PromqlScalarFromVector(child) => {
                self.alloc_passthrough_child(child, "ScalarFromVector", budget, |c| {
                    QueryExpr::PromqlScalarFromVector(Rc::new(c))
                })
            }
            QueryExpr::PromqlRelabel { dst, value, child } => {
                self.alloc_passthrough_child(child, "Relabel", budget, |c| {
                    QueryExpr::PromqlRelabel {
                        dst,
                        value,
                        child: Rc::new(c),
                    }
                })
            }
            QueryExpr::PromqlInfoEnrich { selector, child } => {
                self.alloc_passthrough_child(child, "InfoJoin", budget, |c| {
                    QueryExpr::PromqlInfoEnrich {
                        selector,
                        child: Rc::new(c),
                    }
                })
            }
            QueryExpr::PromqlSeriesSample { by, kind, child } => {
                self.alloc_passthrough_child(child, "Sample", budget, |c| {
                    QueryExpr::PromqlSeriesSample {
                        by,
                        kind,
                        child: Rc::new(c),
                    }
                })
            }
            QueryExpr::TimeShift { shift, child } => {
                self.alloc_passthrough_child(child, "TimeShift", budget, |c| QueryExpr::TimeShift {
                    shift,
                    child: Rc::new(c),
                })
            }
            QueryExpr::SQLWindowFunc {
                func,
                args,
                partition_by,
                order_by,
                output_name,
                frame,
                child,
            } => self.alloc_passthrough_child(child, "WindowFunc", budget, |c| {
                QueryExpr::SQLWindowFunc {
                    func,
                    args,
                    partition_by,
                    order_by,
                    output_name,
                    frame,
                    child: Rc::new(c),
                }
            }),
            // Scalar-expression node (Column/Literal/Compare/BoolAnd/
            // BoolOr/Not/IsNull/IsNotNull/Cast/InList/FunctionCall/Arith/
            // Case) — these live inside `Predicate`/`ProjectItem` scalar
            // positions this deployment's front end builds, never as a
            // bare top-level relational tree node reaching this
            // allocator directly (folded into `QueryExpr` itself by
            // ASAPPlanner#205/#214, see
            // control_plane/docs/design-asapplanner-pin-migration.md).
            // Defensive leaf fallback, matching `Scan`'s treatment,
            // rather than a panic, in case that assumption ever breaks.
            _ => PlanNode::leaf(expr, PipelineStage::Agent, ExecutionMode::Passthrough),
        }
    }

    /// Shared body for the single-child PromQL-surface passthrough arms:
    /// recurse into `child`, rebuild the node via `rebuild`, and stage it
    /// as an informational Agent passthrough carrying the child's cost.
    fn alloc_passthrough_child(
        &self,
        child: Rc<QueryExpr>,
        label: &'static str,
        budget: &mut BudgetState,
        rebuild: impl FnOnce(QueryExpr) -> QueryExpr,
    ) -> PlanNode {
        let child_node = self.alloc_node((*child).clone(), budget);
        PlanNode {
            expr: rebuild(child_node.expr.clone()),
            stage: PipelineStage::Agent,
            mode: ExecutionMode::Passthrough,
            cost: child_node.cost.clone(),
            annotation: NodeAnnotation {
                rationale: format!("{label} at Agent (informational passthrough)"),
                ..Default::default()
            },
            children: vec![child_node],
        }
    }

    // ── Single-intent Aggregate allocation (budget-driven demotion) ───────────

    /// Allocate a single-intent, no-HAVING `Aggregate` — the canonical
    /// shape the legacy `SketchAgg` / `WindowedAgg`-inner-agg folded into.
    /// The caller (the `Aggregate` arm of [`Self::alloc_node`]) guarantees
    /// `aggs.len() == 1` and that the single intent is not `TopK`.
    fn alloc_sketch_agg(
        &self,
        reduction: Reduction,
        aggs: Vec<AggIntent>,
        output_names: Vec<String>,
        child: PlanNode,
        budget: &mut BudgetState,
    ) -> PlanNode {
        let intent = aggs[0].clone();

        // Exact non-mergeable (Avg) → always Db.
        if matches!(intent, AggIntent::Avg { .. }) {
            return PlanNode {
                expr: QueryExpr::Aggregate {
                    reduction,
                    measures: aggs,
                    output_names,
                    having: None,
                    child: Rc::new(child.expr.clone()),
                },
                stage: PipelineStage::Db,
                mode: ExecutionMode::Exact,
                cost: CostEstimate {
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
        if agg_is_exact(&intent) {
            return PlanNode {
                expr: QueryExpr::Aggregate {
                    reduction,
                    measures: aggs,
                    output_names,
                    having: None,
                    child: Rc::new(child.expr.clone()),
                },
                stage: PipelineStage::Backend,
                mode: ExecutionMode::Exact,
                cost: CostEstimate {
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

        // Sketch operators: resolve to physical, then try
        // Agent → Backend → Precompute.
        let physical = super::planner::resolve(&intent);
        let mem = estimated_sketch_memory(&intent);
        let (sketch_type, params) = (physical.sketch_type, physical.sketch_params);

        if budget.fits_agent(mem) {
            budget.consume_agent(mem);
            return PlanNode {
                expr: QueryExpr::Aggregate {
                    reduction,
                    measures: aggs,
                    output_names,
                    having: None,
                    child: Rc::new(child.expr.clone()),
                },
                stage: PipelineStage::Agent,
                mode: ExecutionMode::Sketch,
                cost: CostEstimate {
                    bytes_per_sec: self.raw_bytes_per_sec * 0.05,
                    memory_bytes: mem,
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
                expr: QueryExpr::Aggregate {
                    reduction,
                    measures: aggs,
                    output_names,
                    having: None,
                    child: Rc::new(child.expr.clone()),
                },
                stage: PipelineStage::Backend,
                mode: ExecutionMode::Sketch,
                cost: CostEstimate {
                    bytes_per_sec: self.raw_bytes_per_sec * 0.1,
                    memory_bytes: mem,
                    compression_ratio: 10.0,
                    ..Default::default()
                },
                annotation: NodeAnnotation {
                    sketch_type: Some(sketch_type),
                    sketch_params: Some(params),
                    rationale: "Sketch demoted to Backend (Agent budget exceeded)".into(),
                    budget_demotion: true,
                    ..Default::default()
                },
                children: vec![child],
            };
        }

        // Both Agent and Backend budgets exceeded → Precompute.
        PlanNode {
            expr: QueryExpr::Aggregate {
                reduction,
                measures: aggs,
                output_names,
                having: None,
                child: Rc::new(child.expr.clone()),
            },
            stage: PipelineStage::Precompute,
            mode: ExecutionMode::Sketch,
            cost: CostEstimate {
                bytes_per_sec: self.raw_bytes_per_sec * 0.2,
                memory_bytes: mem,
                compression_ratio: 5.0,
                ..Default::default()
            },
            annotation: NodeAnnotation {
                sketch_type: Some(sketch_type),
                sketch_params: Some(params),
                rationale: "Sketch demoted to Precompute (Agent+Backend budgets exceeded)".into(),
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
/// annotation rationale text.
fn canonical_intent_kind_str(intent: &AggIntent) -> &'static str {
    if crate::planner_selection::as_frequency(intent).is_some() {
        return "frequency";
    }
    match intent {
        AggIntent::Count { .. } => "count",
        AggIntent::Sum { .. } => "sum",
        AggIntent::Min { .. } => "min",
        AggIntent::Max { .. } => "max",
        AggIntent::Avg { .. } => "avg",
        AggIntent::StdDev { .. } => "stddev",
        AggIntent::Variance { .. } => "variance",
        AggIntent::Quantile { .. } => "quantile",
        AggIntent::TopK { .. } => "topk",
        AggIntent::Cardinality { .. } => "cardinality",
        AggIntent::Rate => "rate",
        AggIntent::Increase => "increase",
        AggIntent::Absent => "absent",
        AggIntent::AbsentOverTime => "absent_over_time",
        AggIntent::PresentOverTime => "present_over_time",
        AggIntent::Delta => "delta",
        AggIntent::Deriv => "deriv",
        AggIntent::PredictLinear { .. } => "predict_linear",
        AggIntent::DoubleExpSmoothing { .. } => "double_exponential_smoothing",
        AggIntent::IDelta => "idelta",
        AggIntent::Resets => "resets",
        AggIntent::Changes => "changes",
        AggIntent::HistogramCount => "histogram_count",
        AggIntent::HistogramSum => "histogram_sum",
        AggIntent::HistogramAvg => "histogram_avg",
        AggIntent::HistogramStdDev => "histogram_stddev",
        AggIntent::HistogramStdVar => "histogram_stdvar",
        AggIntent::HistogramFraction { .. } => "histogram_fraction",
        AggIntent::HistogramQuantile { .. } => "histogram_quantile",
        AggIntent::Math(_) => "math",
        AggIntent::TimeFn(_) => "time_fn",
        AggIntent::Group => "group",
        AggIntent::CountValues { .. } => "count_values",
        AggIntent::LastOverTime => "last_over_time",
        AggIntent::FirstOverTime => "first_over_time",
        AggIntent::MadOverTime => "mad_over_time",
        AggIntent::TsOfMinOverTime => "ts_of_min_over_time",
        AggIntent::TsOfMaxOverTime => "ts_of_max_over_time",
        AggIntent::TsOfFirstOverTime => "ts_of_first_over_time",
        AggIntent::TsOfLastOverTime => "ts_of_last_over_time",
        // Unrecognized Extension (not the Frequency one, guarded above).
        AggIntent::Extension { .. } => "extension",
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::plan::{ExecutionMode, PipelineStage};
    use crate::types::{SketchType, StageResourceBudgets};
    use crate::types_v2::AccuracyTarget;
    use crate::planner_selection::default_frequency;
    use planner_types::pre_asap::{default_cardinality, default_quantile};
    use planner_types::pre_asap::{JoinKind, Predicate, QueryExpr, ScalarValue, Schema, Source};

    /// Canonical `Scan` leaf — the L3 counterpart of the legacy
    /// `QueryExpr::Source(SourceSpec { .. })`.
    fn scan(name: &str) -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: name.into(),
            },
            predicates: vec![],
            schema: Schema::default(),
        }
    }

    /// Single-intent, global (`by: []`), no-HAVING `Aggregate` over a
    /// `Scan` — the canonical shape the legacy `SketchAgg` folded into.
    fn agg(intent: AggIntent) -> QueryExpr {
        QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![intent],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(scan("m")),
        }
    }

    fn alloc(budgets: StageResourceBudgets, expr: QueryExpr) -> PlanNode {
        SketchAllocator::new(budgets, 100_000.0).allocate(expr)
    }

    fn unlimited() -> StageResourceBudgets {
        StageResourceBudgets::default()
    }

    fn tight_agent() -> StageResourceBudgets {
        StageResourceBudgets {
            agent_memory_bytes: Some(1), // 1 byte — too small for any sketch
            ..Default::default()
        }
    }

    fn tight_all() -> StageResourceBudgets {
        StageResourceBudgets {
            agent_memory_bytes: Some(1),
            backend_memory_bytes: Some(1),
            ..Default::default()
        }
    }

    // ── Scan / leaf ───────────────────────────────────────────────────────────

    #[test]
    fn scan_goes_to_agent() {
        let node = alloc(unlimited(), scan("cpu"));
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.mode, ExecutionMode::Passthrough);
    }

    // ── Filter ────────────────────────────────────────────────────────────────

    #[test]
    fn filter_at_agent() {
        let expr = QueryExpr::Filter {
            pred: Predicate(Rc::new(QueryExpr::Literal(ScalarValue::Boolean(true)))),
            child: Rc::new(scan("m")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Agent);
    }

    // ── DDSketch within budget → Agent ────────────────────────────────────────

    #[test]
    fn ddsketch_within_budget_goes_to_agent() {
        let node = alloc(unlimited(), agg(default_quantile(0.99)));
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.mode, ExecutionMode::Sketch);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::DDSketch));
    }

    // ── DDSketch tight agent budget → Backend ─────────────────────────────────

    #[test]
    fn ddsketch_agent_budget_exceeded_goes_to_backend() {
        let node = alloc(tight_agent(), agg(default_quantile(0.99)));
        assert_eq!(node.stage, PipelineStage::Backend);
        assert!(node.annotation.budget_demotion);
    }

    // ── DDSketch tight agent+backend → Precompute ─────────────────────────────

    #[test]
    fn ddsketch_all_budgets_exceeded_goes_to_precompute() {
        let node = alloc(tight_all(), agg(default_quantile(0.99)));
        assert_eq!(node.stage, PipelineStage::Precompute);
        assert!(node.annotation.budget_demotion);
    }

    // ── Exact(Avg) → Db ───────────────────────────────────────────────────────

    #[test]
    fn exact_avg_goes_to_db() {
        let node = alloc(unlimited(), agg(AggIntent::Avg { col: None }));
        assert_eq!(node.stage, PipelineStage::Db);
        assert_eq!(node.mode, ExecutionMode::Exact);
    }

    // ── Exact(Sum) → Backend ──────────────────────────────────────────────────

    #[test]
    fn exact_sum_goes_to_backend() {
        let node = alloc(unlimited(), agg(AggIntent::Sum { col: None }));
        assert_eq!(node.stage, PipelineStage::Backend);
        assert_eq!(node.mode, ExecutionMode::Exact);
    }

    // ── TopK → Precompute ─────────────────────────────────────────────────────

    #[test]
    fn topk_goes_to_precompute() {
        let node = alloc(
            unlimited(),
            agg(AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            }),
        );
        assert_eq!(node.stage, PipelineStage::Precompute);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::CountSketch));
    }

    // ── Merge → Backend ───────────────────────────────────────────────────────

    #[test]
    fn merge_goes_to_backend() {
        let expr = QueryExpr::Concat {
            children: vec![scan("a"), scan("b")],
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Backend);
    }

    // ── HLL → Agent ───────────────────────────────────────────────────────────

    #[test]
    fn hll_within_budget_at_agent() {
        let node = alloc(unlimited(), agg(default_cardinality()));
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::HLL));
    }

    // ── Frequency → Agent ─────────────────────────────────────────────────────

    #[test]
    fn frequency_within_budget_at_agent() {
        let node = alloc(unlimited(), agg(default_frequency()));
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.annotation.sketch_type, Some(SketchType::CountSketch));
    }

    // ── Join → Db ─────────────────────────────────────────────────────────────

    #[test]
    fn join_goes_to_db() {
        let expr = QueryExpr::Join {
            kind: JoinKind::Inner,
            pred: Predicate(Rc::new(QueryExpr::Literal(ScalarValue::Boolean(true)))),
            left: Rc::new(scan("orders")),
            right: Rc::new(scan("items")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Db);
    }

    // ── Multi-intent Aggregate → Db (exact) ───────────────────────────────────

    #[test]
    fn multi_intent_aggregate_goes_to_db() {
        let expr = QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![AggIntent::Sum { col: None }, AggIntent::Min { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(scan("m")),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Db);
        assert_eq!(node.mode, ExecutionMode::Exact);
    }

    // ── TimeRange over a single-intent Aggregate (the WindowedAgg fold) ───────

    #[test]
    fn window_over_aggregate_window_passthrough_agg_sketches() {
        // Canonical fold of legacy `WindowedAgg`: TimeRange passthrough at
        // Agent, inner Aggregate does the sketch placement.
        let expr = QueryExpr::TimeRange {
            range: std::time::Duration::from_secs(300),
            child: Rc::new(agg(default_quantile(0.5))),
        };
        let node = alloc(unlimited(), expr);
        assert_eq!(node.stage, PipelineStage::Agent);
        assert_eq!(node.mode, ExecutionMode::Passthrough);
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.children[0].stage, PipelineStage::Agent);
        assert_eq!(node.children[0].mode, ExecutionMode::Sketch);
    }

    // `LetBinding` doesn't exist in the canonical `QueryExpr` anymore
    // (see `optimizer::engine::CommonSubexprElim`'s doc) -- the
    // `let_binding_inherits_body_stage` test that used to exercise it
    // is gone with it.

    // ── Memory estimate helpers ───────────────────────────────────────────────

    #[test]
    fn cardinality_memory_estimate() {
        let mem = estimated_sketch_memory(&default_cardinality());
        assert!(mem > 0.0);
    }

    #[test]
    fn frequency_memory_estimate() {
        let mem = estimated_sketch_memory(&default_frequency());
        assert!(mem > 0.0);
    }

    // ── PlanSummary from allocated tree ──────────────────────────────────────

    #[test]
    fn plan_summary_shows_bandwidth_saved() {
        let node = alloc(unlimited(), agg(default_quantile(0.99)));
        let summary = node.summarise(100_000.0);
        // sketch reduces to ~5% → saved ~95 000 B/s
        assert!(summary.bandwidth_saved_bytes_per_sec > 50_000.0);
        assert!(summary.agent_memory_bytes > 0.0);
    }
}
