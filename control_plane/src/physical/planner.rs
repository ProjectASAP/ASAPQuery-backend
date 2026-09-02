//! Layer 5 — Physical plan IR.
//!
//! Maps the implementation-independent [`AggIntent`] (Layer 3) to concrete
//! sketch implementations and pipeline stages.
//!
//! # Key types
//!
//! - [`PhysicalAggOp`] — resolved AggIntent → concrete SketchType + SketchParams
//! - [`PhysicalOp`] — a physical operator (sketch build, merge, exchange, eval, etc.)
//! - [`PhysicalNode`] — a node in the physical plan tree (operator + placement + cost)
//! - [`Placement`] — where a physical operator runs (Agent, Backend, PromSketch, QueryEngine)
//!
//! Step γ7: the planner consumes the canonical `query_expr::QueryExpr`.
//! The legacy `SketchAgg` / `WindowedAgg` / `TopK` variants are gone — they
//! fold into canonical `Aggregate` / `Window { Aggregate }`. The single
//! `Aggregate` arm dispatches on shape, and the `Window` arm calls
//! [`crate::physical::window_fusion::recognize_windowed_sketch`] to detect
//! the canonical `Window { child: Aggregate }` fold of a legacy
//! `WindowedAgg` and reconstruct the fused `OtelSketchBuild { window }`
//! placement — the window-defines-sketch-lifecycle invariant, now a
//! planner peephole rather than an IR-shape property.

use std::time::Duration;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::query_expr::QueryExpr;
use crate::intent_algebra::schema::ColumnId;
use crate::physical::sketch_catalog;
use crate::physical::window_fusion::{fused_sketch_decision, recognize_windowed_sketch};
use crate::types::{SketchParams, SketchType};

// ── PhysicalAggOp (resolved sketch intent) ──────────────────────────────────

/// A resolved physical aggregation operation.
///
/// This is the output of `resolve()`: concrete sketch implementation
/// chosen for a logical [`AggIntent`].
#[derive(Debug, Clone)]
pub struct PhysicalAggOp {
    /// The logical intent this was derived from.
    pub intent: AggIntent,
    /// Concrete sketch type.
    pub sketch_type: SketchType,
    /// Concrete sketch parameters.
    pub sketch_params: SketchParams,
    /// Estimated memory footprint per series (bytes).
    pub estimated_memory_bytes: u64,
}

/// Resolve an [`AggIntent`] into a [`PhysicalAggOp`] using default mapping.
///
/// This is the Layer 3 → Layer 5 boundary.
pub fn resolve(intent: &AggIntent) -> PhysicalAggOp {
    PhysicalAggOp {
        intent: intent.clone(),
        sketch_type: sketch_catalog::sketch_type_for_op(intent),
        sketch_params: sketch_catalog::sketch_params_for_op(intent),
        estimated_memory_bytes: sketch_catalog::estimated_sketch_memory_bytes(intent),
    }
}

// ── Physical operators ──────────────────────────────────────────────────────

/// A physical operator — concrete implementation of a logical operator.
#[derive(Debug, Clone)]
pub enum PhysicalOp {
    // ── Scan / ingest ─────────────────────────────────────────────
    /// Read raw OTLP metrics from an SDK or scrape target.
    OtlpScan {
        endpoint: String,
        label_matchers: Vec<String>,
    },

    /// Read from an existing PromSketch store.
    PromSketchScan {
        store_addr: String,
        series_selector: String,
    },

    // ── Sketch build ──────────────────────────────────────────────
    /// Build sketch via OTel Collector processor (tumbling window flush).
    OtelSketchBuild {
        sketch_type: SketchType,
        sketch_params: SketchParams,
        window: PhysicalWindow,
        delta_encoding: bool,
    },

    /// Build sketch via PromSketch's ExponentialHistogram layer.
    PromSketchBuild {
        sketch_type: SketchType,
        eh_k: usize,
        time_window: Duration,
    },

    // ── Sketch merge ──────────────────────────────────────────────
    /// Merge sketches from N upstream nodes.
    SketchMerge {
        sketch_type: SketchType,
        group_by: Vec<String>,
    },

    // ── Sketch query ──────────────────────────────────────────────
    /// Extract result from a sketch (quantile, cardinality, frequency).
    SketchEval {
        sketch_type: SketchType,
        func: EvalFunc,
    },

    // ── Data exchange ─────────────────────────────────────────────
    /// Data transfer between pipeline stages.
    Exchange { format: ExchangeFormat },

    // ── Relational / passthrough ──────────────────────────────────
    /// Filter rows.
    Filter { pred: String },
    /// Top-K ranking.
    TopK { k: u64 },
    /// Hash-partitioned aggregation.
    HashAggregate { keys: Vec<String> },
    /// Passthrough — no transformation.
    Passthrough,
}

/// Physical window implementation.
#[derive(Debug, Clone)]
pub enum PhysicalWindow {
    /// OTel Collector: `time.NewTicker` flush + sketch reset.
    OtelTumblingFlush { duration: Duration },
    /// PromSketch ExponentialHistogram: time-decaying buckets.
    PromSketchEH { eh_k: usize, time_window: Duration },
    /// No windowing (unbounded / landmark).
    None,
}

/// What to extract from a sketch at query time.
#[derive(Debug, Clone)]
pub enum EvalFunc {
    Quantile(Vec<f64>),
    Cardinality,
    Frequency { key: String },
    TopK { k: u64 },
    Extrema { min: bool, max: bool },
}

/// Data format for Exchange operators.
#[derive(Debug, Clone)]
pub enum ExchangeFormat {
    /// OTLP gRPC / HTTP.
    Otlp,
    /// Sketch-specific binary (merged sketch bytes).
    SketchBinary,
}

/// Where a physical operator runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// Agent OTel Collector (co-located with SDK).
    AgentCollector,
    /// Backend OTel Collector (merge tier).
    BackendCollector,
    /// PromSketch store (ASAPQuery).
    PromSketchStore,
    /// General query engine (ASAPQuery).
    QueryEngine,
}

// ── Physical plan tree ──────────────────────────────────────────────────────

/// A node in the physical plan tree.
#[derive(Debug, Clone)]
pub struct PhysicalNode {
    /// The physical operator at this node.
    pub op: PhysicalOp,
    /// Where this operator runs.
    pub placement: Placement,
    /// Estimated cost.
    pub cost: PhysicalCost,
    /// Child nodes (ordered: left, right, or input list).
    pub children: Vec<PhysicalNode>,
}

/// Cost estimate for a physical operator.
#[derive(Debug, Clone, Default)]
pub struct PhysicalCost {
    /// Estimated output bandwidth (bytes/sec).
    pub bytes_per_sec: f64,
    /// Estimated memory usage (bytes).
    pub memory_bytes: f64,
    /// Estimated CPU cost (microseconds per sample).
    pub cpu_per_sample: f64,
}

// ── Physical planner ────────────────────────────────────────────────────────

use crate::optimizer::engine::DeploymentConstraints;
use crate::types::StageResourceBudgets;

/// Physical planner configuration.
#[derive(Debug, Clone)]
pub struct PhysicalPlannerConfig {
    pub budgets: StageResourceBudgets,
    pub constraints: DeploymentConstraints,
}

/// Build a physical plan from an optimized canonical [`QueryExpr`].
///
/// Walks the logical tree bottom-up, assigning each node to a pipeline stage
/// (`Placement`), resolving sketch intents to concrete implementations, and
/// inserting `Exchange` nodes at stage boundaries.
///
/// The canonical `Aggregate.by` is already positional, so — unlike the
/// legacy planner — no inherited `Schema` is threaded; placement is
/// purely structural.
pub fn plan(expr: &QueryExpr, config: &PhysicalPlannerConfig) -> PhysicalNode {
    plan_node(expr, config)
}

fn plan_node(expr: &QueryExpr, config: &PhysicalPlannerConfig) -> PhysicalNode {
    match expr {
        // ── Leaf: scan at Agent ─────────────────────────────────────
        QueryExpr::Scan { .. } => PhysicalNode {
            op: PhysicalOp::OtlpScan {
                endpoint: String::new(),
                label_matchers: vec![],
            },
            placement: Placement::AgentCollector,
            cost: PhysicalCost::default(),
            children: vec![],
        },

        // ── Filter: same placement as child ─────────────────────────
        QueryExpr::Filter { pred, child } => {
            let child = plan_node(child, config);
            PhysicalNode {
                placement: child.placement.clone(),
                op: PhysicalOp::Filter {
                    pred: format!("{pred:?}"),
                },
                cost: PhysicalCost::default(),
                children: vec![child],
            }
        }

        // ── TimeRange: a TimeRange over a single-intent Aggregate is the
        // canonical fold of the legacy WindowedAgg (was `Window` before
        // the ASAPPlanner pin migration) — recognize it and reconstruct
        // the fused OtelSketchBuild { window } placement. Any other
        // TimeRange is a plain passthrough inheriting the child's
        // placement.
        QueryExpr::TimeRange {
            child: window_child,
            ..
        } => {
            if let Some(fused) = recognize_windowed_sketch(expr) {
                let child = plan_node(fused.inner_child, config);
                let resolved = resolve(fused.agg);
                let (op, placement) = fused_sketch_decision(&fused, config);
                let mut node = PhysicalNode {
                    op,
                    placement,
                    cost: PhysicalCost {
                        memory_bytes: resolved.estimated_memory_bytes as f64,
                        ..Default::default()
                    },
                    children: vec![child],
                };
                insert_exchange_if_needed(&mut node);
                node
            } else {
                let child = plan_node(window_child, config);
                PhysicalNode {
                    op: PhysicalOp::Passthrough,
                    placement: child.placement.clone(),
                    cost: PhysicalCost::default(),
                    children: vec![child],
                }
            }
        }

        // ── Aggregate: the canonical IR folds legacy SketchAgg /
        // WindowedAgg-inner / TopK all into Aggregate, so dispatch on shape:
        //   * single TopK intent, no HAVING → TopK at QueryEngine
        //   * single other intent, no HAVING → sketch build, budget-placed
        //   * multi-intent or HAVING → exact HashAggregate at QueryEngine
        QueryExpr::Aggregate {
            reduction,
            measures: aggs,
            having,
            child,
            ..
        } => {
            if aggs.len() == 1 && having.is_none() {
                if let AggIntent::TopK { k, .. } = &aggs[0] {
                    let k = *k as u64;
                    let child = plan_node(child, config);
                    let mut node = PhysicalNode {
                        op: PhysicalOp::TopK { k },
                        placement: Placement::QueryEngine,
                        cost: PhysicalCost::default(),
                        children: vec![child],
                    };
                    insert_exchange_if_needed(&mut node);
                    return node;
                }
                // Single non-TopK intent → sketch build (no window — a
                // windowed sketch arrives as `Window { Aggregate }` and is
                // handled by the `Window` arm above).
                let child = plan_node(child, config);
                let resolved = resolve(&aggs[0]);
                let placement = decide_sketch_placement(&resolved, config);
                let mut node = PhysicalNode {
                    op: PhysicalOp::OtelSketchBuild {
                        sketch_type: resolved.sketch_type.clone(),
                        sketch_params: resolved.sketch_params.clone(),
                        window: PhysicalWindow::None,
                        delta_encoding: false,
                    },
                    placement,
                    cost: PhysicalCost {
                        memory_bytes: resolved.estimated_memory_bytes as f64,
                        ..Default::default()
                    },
                    children: vec![child],
                };
                insert_exchange_if_needed(&mut node);
                return node;
            }
            // Multi-intent / HAVING aggregate → no single sketch can serve
            // it; fall back to an exact hash aggregation at the query engine.
            // Always a genuine reduction (never per-entity -- see
            // `intent_algebra::lower`'s single-intent-only per-entity rule).
            let by = reduction.expect_reduce();
            let child = plan_node(child, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::HashAggregate {
                    keys: by.iter().map(|id| format!("{id:?}")).collect(),
                },
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Merge: Backend stage ─────────────────────────────────────
        //
        // `Partition` no longer exists in the canonical IR — its keys
        // fold into `Aggregate.by` at construction time
        // (`intent_algebra::lower`), so the `HashAggregate { keys }` this
        // arm used to build now comes straight out of the `Aggregate`
        // arm above.
        QueryExpr::Concat { children } => {
            let children: Vec<PhysicalNode> =
                children.iter().map(|c| plan_node(c, config)).collect();
            let sketch_type = children
                .first()
                .and_then(|c| match &c.op {
                    PhysicalOp::OtelSketchBuild { sketch_type, .. } => Some(sketch_type.clone()),
                    _ => None,
                })
                .unwrap_or(SketchType::DDSketch);
            PhysicalNode {
                op: PhysicalOp::SketchMerge {
                    sketch_type,
                    group_by: vec![],
                },
                placement: Placement::BackendCollector,
                cost: PhysicalCost::default(),
                children,
            }
        }

        QueryExpr::Dedup { cols, child } => {
            let child = plan_node(child, config);
            let pred = format!("distinct({})", display_distinct_cols(cols));
            let mut node = PhysicalNode {
                op: PhysicalOp::Filter { pred },
                placement: Placement::BackendCollector,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── BinaryOp / Subquery: QueryEngine stage ──────────────────
        QueryExpr::BinaryOp { lhs, rhs, .. } => {
            let left = plan_node(lhs, config);
            let right = plan_node(rhs, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![left, right],
            }
        }

        QueryExpr::PromqlSubquery { child, .. } => {
            let child = plan_node(child, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Sort / Limit / Project: inherit child placement ─────────
        QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::Project { child, .. } => {
            let child = plan_node(child, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: child.placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![child],
            }
        }

        // ── Join / SetOp: both children, QueryEngine placement ──────
        QueryExpr::Join { left, right, .. } | QueryExpr::SetOp { left, right, .. } => {
            let l = plan_node(left, config);
            let r = plan_node(right, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![l, r],
            }
        }

        // The PromQL-surface superset (Scalar/EvalTime/VectorFromScalar/
        // ScalarFromVector/Relabel/InfoJoin/Sample/TimeShift/WindowFunc)
        // isn't constructed by this parser today (`TimeRange` has its own
        // arm above -- it *is* constructed). `Scalar` / `EvalTime` are
        // leaves; every other new variant wraps exactly one child —
        // inherit its placement, mirroring the Sort/Limit/Project arm
        // above, until a dedicated physical op is written.
        QueryExpr::PromqlScalarBridge(_) | QueryExpr::EvalTimestamp => PhysicalNode {
            op: PhysicalOp::Passthrough,
            placement: Placement::QueryEngine,
            cost: PhysicalCost::default(),
            children: vec![],
        },
        QueryExpr::PromqlVectorFromScalar(child)
        | QueryExpr::PromqlScalarFromVector(child)
        | QueryExpr::PromqlRelabel { child, .. }
        | QueryExpr::PromqlInfoEnrich { child, .. }
        | QueryExpr::PromqlSeriesSample { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => {
            let child = plan_node(child, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: child.placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![child],
            }
        }
        // Scalar-expression node (Column/Literal/Compare/BoolAnd/BoolOr/
        // Not/IsNull/IsNotNull/Cast/InList/FunctionCall/Arith/Case) --
        // see `physical::allocator`'s matching catch-all for why this
        // never reaches here as a bare top-level node in practice.
        _ => PhysicalNode {
            op: PhysicalOp::Passthrough,
            placement: Placement::QueryEngine,
            cost: PhysicalCost::default(),
            children: vec![],
        },
    }
}

/// Decide where a sketch runs based on deployment constraints and sketch capability.
///
/// Uses `StageBudget::fits(SketchCapability)` to check each stage in order:
/// Agent → BackendCollector → QueryEngine.
pub(crate) fn decide_sketch_placement(
    resolved: &PhysicalAggOp,
    config: &PhysicalPlannerConfig,
) -> Placement {
    use crate::optimizer::engine::sketch_capability;

    let cap = sketch_capability(&resolved.sketch_type);

    // Try Agent first.
    if config.constraints.agent.fits(&cap) {
        return Placement::AgentCollector;
    }

    // Agent budget exceeded — try Backend.
    if config.constraints.backend_collector.fits(&cap) {
        return Placement::BackendCollector;
    }

    // Both exceeded — defer to QueryEngine.
    Placement::QueryEngine
}

/// Render a `Distinct { cols }` column tuple into a human-readable display
/// string for the `Filter { pred }` rationale. `Distinct { cols: [] }` is
/// whole-row SQL DISTINCT and prints as `*`. Canonical `cols` are
/// positional `ColumnId`s (no name at this layer — `plan_node` has no
/// schema in scope to resolve one), so each renders as `col#<id>`.
fn display_distinct_cols(cols: &[ColumnId]) -> String {
    if cols.is_empty() {
        return "*".into();
    }
    cols.iter()
        .map(|id| format!("col#{id}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// If a node's child is at a different stage, insert an Exchange node between them.
fn insert_exchange_if_needed(node: &mut PhysicalNode) {
    let parent_placement = node.placement.clone();
    for child in &mut node.children {
        if child.placement != parent_placement {
            let format = match (&child.placement, &parent_placement) {
                (Placement::AgentCollector, Placement::BackendCollector) => ExchangeFormat::Otlp,
                (Placement::AgentCollector, Placement::QueryEngine) => ExchangeFormat::Otlp,
                (Placement::BackendCollector, Placement::QueryEngine) => {
                    ExchangeFormat::SketchBinary
                }
                _ => ExchangeFormat::Otlp,
            };
            // Wrap the child in an Exchange node
            let original_child = std::mem::replace(
                child,
                PhysicalNode {
                    op: PhysicalOp::Passthrough,
                    placement: parent_placement.clone(),
                    cost: PhysicalCost::default(),
                    children: vec![],
                },
            );
            *child = PhysicalNode {
                op: PhysicalOp::Exchange { format },
                placement: parent_placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![original_child],
            };
        }
    }
}

impl PhysicalNode {
    /// Count total nodes in the tree.
    pub fn node_count(&self) -> usize {
        1 + self.children.iter().map(|c| c.node_count()).sum::<usize>()
    }

    /// Collect all distinct placements in the tree.
    pub fn placements(&self) -> Vec<Placement> {
        let mut out = vec![self.placement.clone()];
        for child in &self.children {
            for p in child.placements() {
                if !out.contains(&p) {
                    out.push(p);
                }
            }
        }
        out
    }

    /// Count Exchange nodes (= stage boundary crossings).
    pub fn exchange_count(&self) -> usize {
        let self_count = if matches!(self.op, PhysicalOp::Exchange { .. }) {
            1
        } else {
            0
        };
        self_count
            + self
                .children
                .iter()
                .map(|c| c.exchange_count())
                .sum::<usize>()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::intent_algebra::relational::{
        default_cardinality, default_frequency, default_quantile,
    };
    use crate::intent_algebra::{Reduction, Schema, Source};
    use crate::types_v2::AccuracyTarget;

    fn default_config() -> PhysicalPlannerConfig {
        PhysicalPlannerConfig {
            budgets: StageResourceBudgets::default(),
            constraints: DeploymentConstraints::default(),
        }
    }

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

    /// Single-intent, global, no-HAVING `Aggregate` over a `Scan` — the
    /// canonical fold of the legacy `SketchAgg`.
    fn sketch_agg(intent: AggIntent, metric: &str) -> QueryExpr {
        QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![intent],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(scan(metric)),
        }
    }

    /// `TimeRange { Aggregate }` — the canonical fold of the legacy
    /// `WindowedAgg`.
    fn windowed_agg(intent: AggIntent, size_secs: u64, metric: &str) -> QueryExpr {
        QueryExpr::TimeRange {
            range: Duration::from_secs(size_secs),
            child: Rc::new(sketch_agg(intent, metric)),
        }
    }

    #[test]
    fn resolve_quantile() {
        let p = resolve(&default_quantile(0.99));
        assert_eq!(p.sketch_type, SketchType::DDSketch);
        assert!(matches!(p.sketch_params, SketchParams::DDSketch { .. }));
        assert!(p.estimated_memory_bytes > 0);
    }

    #[test]
    fn resolve_cardinality() {
        let p = resolve(&default_cardinality());
        assert_eq!(p.sketch_type, SketchType::HLL);
        assert!(matches!(p.sketch_params, SketchParams::HLL { .. }));
    }

    #[test]
    fn resolve_frequency() {
        let p = resolve(&default_frequency());
        assert_eq!(p.sketch_type, SketchType::CountSketch);
        assert!(matches!(p.sketch_params, SketchParams::CountSketch { .. }));
    }

    #[test]
    fn resolve_preserves_intent() {
        let intent = AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.005),
        };
        let p = resolve(&intent);
        assert_eq!(p.intent, intent);
    }

    // ── Physical planner tests ──────────────────────────────────────────

    #[test]
    fn plan_simple_sketch_at_agent() {
        // Aggregate { Quantile } over Scan → Agent placement
        let expr = sketch_agg(default_quantile(0.99), "m");
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::AgentCollector);
        assert!(matches!(node.op, PhysicalOp::OtelSketchBuild { .. }));
        assert_eq!(node.children.len(), 1); // Scan child
    }

    #[test]
    fn plan_windowed_agg_has_window() {
        // Window { Aggregate { Quantile } } → fused OtelSketchBuild with a
        // resolved tumbling window (the WindowedAgg fold; PR-5 recognizer).
        let expr = windowed_agg(default_quantile(0.5), 300, "m");
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::AgentCollector);
        match &node.op {
            PhysicalOp::OtelSketchBuild { window, .. } => {
                assert!(matches!(window, PhysicalWindow::OtelTumblingFlush { .. }));
            }
            other => panic!("expected OtelSketchBuild, got {other:?}"),
        }
    }

    #[test]
    fn plan_topk_at_query_engine() {
        let expr = QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(sketch_agg(default_frequency(), "m")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::QueryEngine);
        assert!(matches!(node.op, PhysicalOp::TopK { k: 10 }));
    }

    #[test]
    fn plan_topk_inserts_exchange() {
        // TopK(QueryEngine) wrapping a sketch Aggregate(Agent) → Exchange
        // between them.
        let expr = QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![AggIntent::TopK {
                k: 5,
                accuracy: AccuracyTarget::Epsilon(0.05),
            }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(sketch_agg(default_frequency(), "m")),
        };
        let node = plan(&expr, &default_config());
        assert!(
            node.exchange_count() > 0,
            "expected Exchange between Agent and QueryEngine"
        );
    }

    // `plan_partition_at_backend` (a standalone `Partition` node always
    // placing at `BackendCollector`) is removed: `Partition` no longer
    // exists in the canonical IR — its keys fold into `Aggregate.by` at
    // construction time (`intent_algebra::lower`), and a grouped
    // single-intent `Aggregate` goes through the same budget-driven
    // Agent→Backend→Precompute path as an ungrouped one (`by` isn't a
    // placement input), so there is no direct equivalent assertion to
    // make here.

    #[test]
    fn plan_multi_intent_aggregate_at_query_engine() {
        // Multi-intent Aggregate → exact HashAggregate at QueryEngine
        // (no single sketch serves multiple intents).
        let expr = QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![AggIntent::Sum { col: None }, AggIntent::Min { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(scan("trades")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::QueryEngine);
        assert!(matches!(node.op, PhysicalOp::HashAggregate { .. }));
    }

    #[test]
    fn plan_full_pipeline_has_multiple_stages() {
        // TopK(Merge(Window(Aggregate(Scan)))) — `windowed_agg` fuses to
        // Agent, `Merge` (the `Frequency` intent's sketch fan-in) sits at
        // Backend, and the outer `TopK` intent runs at QueryEngine.
        // Should span: Agent → Backend → QueryEngine
        let expr = QueryExpr::Aggregate {
            reduction: Reduction::by(vec![]),
            measures: vec![AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            }],
            output_names: Vec::new(),
            having: None,
            child: QueryExpr::Concat {
                children: vec![windowed_agg(default_frequency(), 60, "requests")],
            }
            .into(),
        };
        let node = plan(&expr, &default_config());
        let placements = node.placements();
        assert!(
            placements.contains(&Placement::AgentCollector),
            "should have Agent: {placements:?}"
        );
        assert!(
            placements.contains(&Placement::BackendCollector),
            "should have Backend: {placements:?}"
        );
        assert!(
            placements.contains(&Placement::QueryEngine),
            "should have QueryEngine: {placements:?}"
        );
        // Was `>= 2` when `Merge`'s child used to be a `Partition` node
        // (which called `insert_exchange_if_needed` itself, contributing
        // a second exchange on top of the outer TopK Aggregate's own).
        // `Merge` doesn't call `insert_exchange_if_needed` on its
        // children — it accepts heterogeneously-placed children by
        // design (see its arm above) — so with `Partition` gone (its
        // keys fold into `Aggregate.by` at construction time) this shape
        // now has exactly one exchange-inserting boundary: the outer
        // TopK Aggregate against its `Merge` child. The 3-stage span
        // above is still the real invariant this test protects.
        assert!(
            node.exchange_count() >= 1,
            "should have >=1 exchange: {}",
            node.exchange_count()
        );
    }

    #[test]
    fn plan_budget_deferral() {
        // With tiny agent budget, sketch should defer to Backend.
        let budgets = StageResourceBudgets {
            agent_memory_bytes: Some(1), // 1 byte = too small
            ..Default::default()
        };
        let config = PhysicalPlannerConfig {
            constraints: DeploymentConstraints::from_budgets(&budgets),
            budgets,
        };
        let expr = sketch_agg(default_quantile(0.99), "m");
        let node = plan(&expr, &config);
        assert_eq!(
            node.placement,
            Placement::BackendCollector,
            "sketch should be deferred to Backend when agent budget is tiny"
        );
    }
}
