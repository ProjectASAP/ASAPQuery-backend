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
//! - [`Placement`] — where a physical operator runs (Agent, Backend, PromSketch, DB, etc.)

use std::time::Duration;

use crate::physical::sketch_catalog;
use crate::intent_algebra::legacy_expr::{AggIntent, WindowKind, WindowSpec};
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
    Exchange {
        format: ExchangeFormat,
    },

    // ── Relational / passthrough ──────────────────────────────────
    /// Filter rows.
    Filter { pred: String },
    /// Top-K ranking.
    TopK { k: u64 },
    /// Hash-partitioned aggregation.
    HashAggregate { keys: Vec<String> },
    /// SQL query to database.
    DbQuery { sql: String },
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
    /// Database-side: `GROUP BY time_bucket(interval, ts)`.
    SqlTimeBucket { interval: Duration, time_col: String },
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
    /// Raw samples (for non-sketch path).
    RawSamples,
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
    /// Database (ClickHouse, TimescaleDB, etc.).
    Database,
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

// ── Window resolution ───────────────────────────────────────────────────────

/// Resolve a logical [`WindowSpec`] to a [`PhysicalWindow`] for a given placement.
pub fn resolve_window(window: &WindowSpec, placement: &Placement) -> PhysicalWindow {
    match (&window.kind, placement) {
        (WindowKind::Tumbling { size }, Placement::AgentCollector) =>
            PhysicalWindow::OtelTumblingFlush { duration: *size },
        (WindowKind::Tumbling { size }, Placement::PromSketchStore) =>
            PhysicalWindow::PromSketchEH { eh_k: 50, time_window: *size },
        (WindowKind::Sliding { size, .. }, Placement::PromSketchStore) =>
            PhysicalWindow::PromSketchEH { eh_k: 50, time_window: *size },
        (WindowKind::Tumbling { size }, Placement::Database) =>
            PhysicalWindow::SqlTimeBucket {
                interval: *size,
                time_col: window.time_col.clone().unwrap_or_else(|| "ts".into()),
            },
        (WindowKind::Unbounded | WindowKind::Landmark, _) =>
            PhysicalWindow::None,
        // Fallback: tumbling at the given size for any other combo.
        (WindowKind::Tumbling { size } | WindowKind::Sliding { size, .. } | WindowKind::Session { gap: size }, _) =>
            PhysicalWindow::OtelTumblingFlush { duration: *size },
    }
}

// ── Physical planner ────────────────────────────────────────────────────────

use crate::intent_algebra::legacy_expr::*;
use crate::optimizer::engine::DeploymentConstraints;
use crate::types::StageResourceBudgets;

/// Physical planner configuration.
#[derive(Debug, Clone)]
pub struct PhysicalPlannerConfig {
    pub budgets: StageResourceBudgets,
    pub constraints: DeploymentConstraints,
}

/// Build a physical plan from an optimized `QueryExpr`.
///
/// Walks the logical tree bottom-up, assigning each node to a pipeline stage
/// (`Placement`), resolving sketch intents to concrete implementations, and
/// inserting `Exchange` nodes at stage boundaries.
pub fn plan(expr: &QueryExpr, config: &PhysicalPlannerConfig) -> PhysicalNode {
    plan_node(expr, config)
}

fn plan_node(expr: &QueryExpr, config: &PhysicalPlannerConfig) -> PhysicalNode {
    match expr {
        // ── Leaf: scan at Agent ─────────────────────────────────────
        QueryExpr::Source(s) => PhysicalNode {
            op: PhysicalOp::OtlpScan {
                endpoint: String::new(),
                label_matchers: vec![],
            },
            placement: Placement::AgentCollector,
            cost: PhysicalCost::default(),
            children: vec![],
        },

        // ── Filter: same placement as child ─────────────────────────
        QueryExpr::Filter { pred, input } => {
            let child = plan_node(input, config);
            PhysicalNode {
                placement: child.placement.clone(),
                op: PhysicalOp::Filter { pred: format!("{pred:?}") },
                cost: PhysicalCost::default(),
                children: vec![child],
            }
        }

        // ── SketchAgg: resolve intent → physical, place at Agent or defer ──
        QueryExpr::SketchAgg { op, col, input } => {
            let child = plan_node(input, config);
            let resolved = resolve(op);
            let placement = decide_sketch_placement(&resolved, config);

            let physical_op = PhysicalOp::OtelSketchBuild {
                sketch_type: resolved.sketch_type.clone(),
                sketch_params: resolved.sketch_params.clone(),
                window: PhysicalWindow::None,
                delta_encoding: false,
            };

            let mut node = PhysicalNode {
                op: physical_op,
                placement: placement.clone(),
                cost: PhysicalCost {
                    memory_bytes: resolved.estimated_memory_bytes as f64,
                    ..Default::default()
                },
                children: vec![child],
            };
            // Insert exchange if child is at a different stage
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── WindowedAgg: resolve + place with window ────────────────
        QueryExpr::WindowedAgg { agg, window, col, input } => {
            let child = plan_node(input, config);
            let resolved = resolve(agg);
            let placement = decide_sketch_placement(&resolved, config);
            let phys_window = resolve_window(window, &placement);

            let physical_op = PhysicalOp::OtelSketchBuild {
                sketch_type: resolved.sketch_type.clone(),
                sketch_params: resolved.sketch_params.clone(),
                window: phys_window,
                delta_encoding: false,
            };

            let mut node = PhysicalNode {
                op: physical_op,
                placement: placement.clone(),
                cost: PhysicalCost {
                    memory_bytes: resolved.estimated_memory_bytes as f64,
                    ..Default::default()
                },
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Partition / Merge: Backend stage ────────────────────────
        QueryExpr::Partition { keys, input } => {
            let child = plan_node(input, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::HashAggregate { keys: keys.keys().to_vec() },
                placement: Placement::BackendCollector,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        QueryExpr::Merge { inputs } => {
            let children: Vec<PhysicalNode> = inputs.iter()
                .map(|i| plan_node(i, config))
                .collect();
            let sketch_type = children.first()
                .and_then(|c| match &c.op {
                    PhysicalOp::OtelSketchBuild { sketch_type, .. } => Some(sketch_type.clone()),
                    _ => None,
                })
                .unwrap_or(SketchType::DDSketch);
            PhysicalNode {
                op: PhysicalOp::SketchMerge { sketch_type, group_by: vec![] },
                placement: Placement::BackendCollector,
                cost: PhysicalCost::default(),
                children,
            }
        }

        QueryExpr::Distinct { cols, input } => {
            let child = plan_node(input, config);
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

        // ── TopK / HistogramQuantile / BinaryOp: QueryEngine stage ──
        QueryExpr::TopK { k, input, .. } => {
            let child = plan_node(input, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::TopK { k: *k },
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        QueryExpr::HistogramQuantile { phi, input } => {
            let child = plan_node(input, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::SketchEval {
                    sketch_type: SketchType::DDSketch,
                    func: EvalFunc::Quantile(vec![*phi]),
                },
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        QueryExpr::BinaryOp { op, lhs, rhs, .. } => {
            let left = plan_node(lhs, config);
            let right = plan_node(rhs, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![left, right],
            }
        }

        QueryExpr::PromQLSubquery { input, .. } => {
            let child = plan_node(input, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Aggregate (non-sketch, exact): Database stage ───────────
        QueryExpr::Aggregate { keys, input, .. } => {
            let child = plan_node(input, config);
            let mut node = PhysicalNode {
                op: PhysicalOp::DbQuery { sql: format!("GROUP BY {:?}", keys) },
                placement: Placement::Database,
                cost: PhysicalCost::default(),
                children: vec![child],
            };
            insert_exchange_if_needed(&mut node);
            node
        }

        // ── Sort / Limit / Project: inherit child placement ─────────
        QueryExpr::Sort { input, .. }
        | QueryExpr::Limit { input, .. }
        | QueryExpr::Project { input, .. }
        | QueryExpr::Window { input, .. } => {
            let child = plan_node(input, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: child.placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![child],
            }
        }

        // ── Join: both children, QueryEngine placement ──────────────
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. } => {
            let l = plan_node(left, config);
            let r = plan_node(right, config);
            PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: Placement::QueryEngine,
                cost: PhysicalCost::default(),
                children: vec![l, r],
            }
        }

        // ── LetBinding ──────────────────────────────────────────────
        QueryExpr::LetBinding { body, .. } => plan_node(body, config),
        QueryExpr::Ref(_) => PhysicalNode {
            op: PhysicalOp::Passthrough,
            placement: Placement::QueryEngine,
            cost: PhysicalCost::default(),
            children: vec![],
        },
    }
}

/// Decide where a sketch operation runs based on memory budget.
/// Decide where a sketch runs based on deployment constraints and sketch capability.
///
/// Uses `StageBudget::fits(SketchCapability)` to check each stage in order:
/// Agent → BackendCollector → QueryEngine.
fn decide_sketch_placement(resolved: &PhysicalAggOp, config: &PhysicalPlannerConfig) -> Placement {
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

/// If a node's child is at a different stage, insert an Exchange node between them.
/// Render a `Distinct { cols }` column tuple into a human-readable display
/// string for the `Filter { pred }` rationale. `Distinct { cols: [] }` is
/// whole-row SQL DISTINCT and prints as `*`.
fn display_distinct_cols(cols: &[ColumnRef]) -> String {
    if cols.is_empty() {
        return "*".into();
    }
    cols.iter()
        .map(|c| match c {
            ColumnRef::Named(s) => s.clone(),
            ColumnRef::SampleValue => "@value".into(),
            ColumnRef::Wildcard => "*".into(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn insert_exchange_if_needed(node: &mut PhysicalNode) {
    let parent_placement = node.placement.clone();
    for child in &mut node.children {
        if child.placement != parent_placement {
            let format = match (&child.placement, &parent_placement) {
                (Placement::AgentCollector, Placement::BackendCollector) => ExchangeFormat::Otlp,
                (Placement::AgentCollector, Placement::QueryEngine) => ExchangeFormat::Otlp,
                (Placement::BackendCollector, Placement::QueryEngine) => ExchangeFormat::SketchBinary,
                (Placement::AgentCollector, Placement::Database) => ExchangeFormat::RawSamples,
                _ => ExchangeFormat::Otlp,
            };
            // Wrap the child in an Exchange node
            let original_child = std::mem::replace(child, PhysicalNode {
                op: PhysicalOp::Passthrough,
                placement: parent_placement.clone(),
                cost: PhysicalCost::default(),
                children: vec![],
            });
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
        let self_count = if matches!(self.op, PhysicalOp::Exchange { .. }) { 1 } else { 0 };
        self_count + self.children.iter().map(|c| c.exchange_count()).sum::<usize>()
    }

    /// Extract a flat [`StagedPlan`] from this physical plan tree.
    ///
    /// Walks the tree and populates each sub-plan based on node placement
    /// and operator type.  This bridges the physical planner to the existing
    /// config generators that consume `StagedPlan`.
    pub fn to_staged_plan(&self) -> crate::types::StagedPlan {
        use crate::types::{
            AgentSubPlan, BackendSubPlan, DbSubPlan, PrecomputeSubPlan, StagedPlan,
        };

        let mut staged = StagedPlan::default();
        self.collect_into_staged(&mut staged);
        staged
    }

    fn collect_into_staged(&self, staged: &mut crate::types::StagedPlan) {
        use crate::types::StagedPlan;

        match (&self.placement, &self.op) {
            // Agent: sketch build → populate agent sub-plan
            (Placement::AgentCollector, PhysicalOp::OtelSketchBuild {
                sketch_type, sketch_params, window, ..
            }) => {
                staged.agent.sketch_type = Some(sketch_type.clone());
                staged.agent.sketch_params = sketch_params.clone();
                if let PhysicalWindow::OtelTumblingFlush { duration } = window {
                    staged.agent.window_secs = Some(duration.as_secs());
                }
            }

            // Agent: filter → label filters
            (Placement::AgentCollector, PhysicalOp::Filter { pred }) => {
                staged.agent.label_filters.push(pred.clone());
            }

            // Backend: merge/aggregate
            (Placement::BackendCollector, PhysicalOp::SketchMerge { group_by, .. }) => {
                staged.backend.has_merge = true;
                staged.backend.group_by = group_by.clone();
            }
            (Placement::BackendCollector, PhysicalOp::HashAggregate { keys }) => {
                staged.backend.has_merge = true;
                staged.backend.group_by = keys.clone();
            }
            (Placement::BackendCollector, PhysicalOp::Filter { .. }) => {
                staged.backend.has_dedup = true;
            }

            // QueryEngine: TopK, SketchEval
            (Placement::QueryEngine, PhysicalOp::TopK { k }) => {
                staged.precompute.active = true;
                staged.precompute.topk = Some(*k);
            }
            (Placement::QueryEngine, PhysicalOp::SketchEval { .. }) => {
                staged.precompute.active = true;
            }
            (Placement::QueryEngine, PhysicalOp::Passthrough) => {
                staged.precompute.active = true;
            }

            // Database: exact computation
            (Placement::Database, PhysicalOp::DbQuery { sql }) => {
                staged.db.active = true;
                staged.db.query_expr = sql.clone();
            }

            // Exchange: record deferral
            (_, PhysicalOp::Exchange { format }) => {
                staged.deferral_log.push(format!("Exchange({:?})", format));
            }

            _ => {}
        }

        // Recurse into children
        for child in &self.children {
            child.collect_into_staged(staged);
        }
    }
}

// ── Public entry point for main.rs ──────────────────────────────────────────

/// Run the full physical planning pipeline: optimize → plan → staged plan.
///
/// This is the single function `main.rs` calls to get a `StagedPlan`
/// from a parsed `QueryExpr`.
pub fn physical_plan_to_staged(
    expr: &QueryExpr,
    budgets: &StageResourceBudgets,
) -> (crate::types::StagedPlan, PhysicalNode) {
    let constraints = DeploymentConstraints::from_budgets(budgets);
    let config = PhysicalPlannerConfig {
        budgets: budgets.clone(),
        constraints,
    };
    let tree = plan(expr, &config);
    let staged = tree.to_staged_plan();
    (staged, tree)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_quantile() {
        let p = resolve(&AggIntent::default_quantile(vec![0.99]));
        assert_eq!(p.sketch_type, SketchType::DDSketch);
        assert!(matches!(p.sketch_params, SketchParams::DDSketch { .. }));
        assert!(p.estimated_memory_bytes > 0);
    }

    #[test]
    fn resolve_cardinality() {
        let p = resolve(&AggIntent::default_cardinality());
        assert_eq!(p.sketch_type, SketchType::HLL);
        assert!(matches!(p.sketch_params, SketchParams::HLL { .. }));
    }

    #[test]
    fn resolve_frequency() {
        let p = resolve(&AggIntent::default_frequency());
        assert_eq!(p.sketch_type, SketchType::CountSketch);
        assert!(matches!(p.sketch_params, SketchParams::CountSketch { .. }));
    }

    #[test]
    fn resolve_preserves_intent() {
        let intent = AggIntent::Quantile { quantiles: vec![0.5, 0.99], accuracy: 0.005 };
        let p = resolve(&intent);
        assert_eq!(p.intent, intent);
    }

    #[test]
    fn tumbling_window_at_agent() {
        let ws = WindowSpec {
            kind: WindowKind::Tumbling { size: Duration::from_secs(300) },
            time_col: None,
        };
        let pw = resolve_window(&ws, &Placement::AgentCollector);
        assert!(matches!(pw, PhysicalWindow::OtelTumblingFlush { .. }));
    }

    #[test]
    fn sliding_window_at_promsketch() {
        let ws = WindowSpec {
            kind: WindowKind::Sliding { size: Duration::from_secs(300), slide: Duration::from_secs(60) },
            time_col: None,
        };
        let pw = resolve_window(&ws, &Placement::PromSketchStore);
        assert!(matches!(pw, PhysicalWindow::PromSketchEH { .. }));
    }

    #[test]
    fn tumbling_window_at_database() {
        let ws = WindowSpec {
            kind: WindowKind::Tumbling { size: Duration::from_secs(60) },
            time_col: Some("event_time".into()),
        };
        let pw = resolve_window(&ws, &Placement::Database);
        match pw {
            PhysicalWindow::SqlTimeBucket { interval, time_col } => {
                assert_eq!(interval, Duration::from_secs(60));
                assert_eq!(time_col, "event_time");
            }
            other => panic!("expected SqlTimeBucket, got {other:?}"),
        }
    }

    #[test]
    fn unbounded_window_is_none() {
        let ws = WindowSpec { kind: WindowKind::Unbounded, time_col: None };
        let pw = resolve_window(&ws, &Placement::AgentCollector);
        assert!(matches!(pw, PhysicalWindow::None));
    }

    // ── Physical planner tests ──────────────────────────────────────────

    fn default_config() -> PhysicalPlannerConfig {
        PhysicalPlannerConfig {
            budgets: StageResourceBudgets::default(),
            constraints: DeploymentConstraints::default(),
        }
    }

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    #[test]
    fn plan_simple_sketch_at_agent() {
        // SketchAgg { Quantile, Source } → Agent placement
        let expr = QueryExpr::SketchAgg {
            op: AggIntent::default_quantile(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::AgentCollector);
        assert!(matches!(node.op, PhysicalOp::OtelSketchBuild { .. }));
        assert_eq!(node.children.len(), 1); // Source child
    }

    #[test]
    fn plan_windowed_agg_has_window() {
        let expr = QueryExpr::WindowedAgg {
            agg: AggIntent::default_quantile(vec![0.5]),
            window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(300) }, time_col: None },
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
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
        let expr = QueryExpr::TopK {
            k: 10,
            by: vec!["svc".into()],
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_frequency(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::QueryEngine);
        assert!(matches!(node.op, PhysicalOp::TopK { k: 10 }));
    }

    #[test]
    fn plan_topk_inserts_exchange() {
        // TopK(QueryEngine) wrapping SketchAgg(Agent) → Exchange between them
        let expr = QueryExpr::TopK {
            k: 5,
            by: vec![],
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_frequency(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert!(node.exchange_count() > 0, "expected Exchange between Agent and QueryEngine");
    }

    #[test]
    fn plan_partition_at_backend() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["region".into()]),
            input: Box::new(QueryExpr::SketchAgg {
                op: AggIntent::default_cardinality(),
                col: ColumnRef::SampleValue,
                input: Box::new(src("m")),
            }),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::BackendCollector);
    }

    #[test]
    fn plan_aggregate_at_database() {
        let expr = QueryExpr::Aggregate {
            keys: vec!["symbol".into()],
            aggs: vec![AggItem {
                alias: "avg".into(),
                func: AggFunc::Avg,
                col: ColumnRef::Named("price".into()),
                distinct: false,
            }],
            having: None,
            input: Box::new(src("trades")),
        };
        let node = plan(&expr, &default_config());
        assert_eq!(node.placement, Placement::Database);
    }

    #[test]
    fn plan_full_pipeline_has_multiple_stages() {
        // TopK(Partition(WindowedAgg(Filter(Source))))
        // Should span: Agent → Backend → QueryEngine
        let expr = QueryExpr::TopK {
            k: 10,
            by: vec!["svc".into()],
            input: Box::new(QueryExpr::Partition {
                keys: PartitionKeys::By(vec!["svc".into()]),
                input: Box::new(QueryExpr::WindowedAgg {
                    agg: AggIntent::default_frequency(),
                    window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(60) }, time_col: None },
                    col: ColumnRef::SampleValue,
                    input: Box::new(QueryExpr::Filter {
                        pred: ScalarExpr::Literal(LiteralValue::Bool(true)),
                        input: Box::new(src("requests")),
                    }),
                }),
            }),
        };
        let node = plan(&expr, &default_config());
        let placements = node.placements();
        assert!(placements.contains(&Placement::AgentCollector), "should have Agent: {placements:?}");
        assert!(placements.contains(&Placement::BackendCollector), "should have Backend: {placements:?}");
        assert!(placements.contains(&Placement::QueryEngine), "should have QueryEngine: {placements:?}");
        assert!(node.exchange_count() >= 2, "should have ≥2 exchanges: {}", node.exchange_count());
    }

    #[test]
    fn plan_budget_deferral() {
        // With tiny agent budget, sketch should defer to Backend
        let budgets = StageResourceBudgets {
            agent_memory_bytes: Some(1), // 1 byte = too small
            ..Default::default()
        };
        let config = PhysicalPlannerConfig {
            constraints: DeploymentConstraints::from_budgets(&budgets),
            budgets,
        };
        let expr = QueryExpr::SketchAgg {
            op: AggIntent::default_quantile(vec![0.99]),
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let node = plan(&expr, &config);
        assert_eq!(node.placement, Placement::BackendCollector,
            "sketch should be deferred to Backend when agent budget is tiny");
    }

    // ── to_staged_plan tests ────────────────────────────────────────────

    #[test]
    fn staged_plan_simple_sketch() {
        let expr = QueryExpr::WindowedAgg {
            agg: AggIntent::Quantile { quantiles: vec![0.99], accuracy: 0.01 },
            window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(300) }, time_col: None },
            col: ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        let (staged, _) = physical_plan_to_staged(&expr, &StageResourceBudgets::default());
        assert_eq!(staged.agent.sketch_type, Some(SketchType::DDSketch));
        assert_eq!(staged.agent.window_secs, Some(300));
        assert!(!staged.precompute.active);
        assert!(!staged.db.active);
    }

    #[test]
    fn staged_plan_topk_multi_stage() {
        let expr = QueryExpr::TopK {
            k: 10,
            by: vec!["svc".into()],
            input: Box::new(QueryExpr::Partition {
                keys: PartitionKeys::By(vec!["svc".into()]),
                input: Box::new(QueryExpr::WindowedAgg {
                    agg: AggIntent::default_frequency(),
                    window: WindowSpec { kind: WindowKind::Tumbling { size: Duration::from_secs(60) }, time_col: None },
                    col: ColumnRef::SampleValue,
                    input: Box::new(src("m")),
                }),
            }),
        };
        let (staged, tree) = physical_plan_to_staged(&expr, &StageResourceBudgets::default());
        // Agent has sketch
        assert!(staged.agent.sketch_type.is_some());
        // Backend has merge
        assert!(staged.backend.has_merge);
        // Precompute has topk
        assert!(staged.precompute.active);
        assert_eq!(staged.precompute.topk, Some(10));
        // Exchanges recorded in deferral log
        assert!(tree.exchange_count() >= 2);
    }

    #[test]
    fn staged_plan_exact_agg_at_db() {
        let expr = QueryExpr::Aggregate {
            keys: vec!["symbol".into()],
            aggs: vec![AggItem {
                alias: "avg".into(),
                func: AggFunc::Avg,
                col: ColumnRef::Named("price".into()),
                distinct: false,
            }],
            having: None,
            input: Box::new(src("trades")),
        };
        let (staged, _) = physical_plan_to_staged(&expr, &StageResourceBudgets::default());
        assert!(staged.db.active);
        assert!(staged.agent.sketch_type.is_none());
    }
}
