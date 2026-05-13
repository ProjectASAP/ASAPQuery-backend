//! Annotated plan nodes — the output of the [`super::allocator::SketchAllocator`].
//!
//! After the optimizer rewrites a [`QueryExpr`](crate::intent_algebra::legacy_expr::QueryExpr) tree,
//! the allocator wraps every node in a [`PlanNode`] that carries:
//!
//! * **`stage`** — which pipeline component executes this operator.
//! * **`mode`** — whether the operator uses sketch approximation or exact
//!   computation.
//! * **`cost`** — estimated memory and bandwidth cost at this node.
//! * **`annotation`** — additional hints for the code-generator (e.g. which
//!   sketch type to use, whether delta encoding is enabled).
//!
//! The annotated plan tree is serialisable to JSON so it can be included in
//! the `/api/v1/plan` response for observability.

use serde::{Deserialize, Serialize};

use crate::intent_algebra::legacy_expr::QueryExpr;

// ── Pipeline stages ───────────────────────────────────────────────────────────

/// Which component in the data pipeline executes an operator.
///
/// The ordering `Agent < Backend < Precompute < Db` mirrors the data-flow
/// direction: data originates at the Agent and flows toward the Db.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStage {
    /// SDK-side OTel Collector — highest bandwidth savings, lowest latency.
    Agent,
    /// Central merge collector — aggregates partial sketches from many agents.
    Backend,
    /// ASAPQuery pre-computation engine — materialises recurring queries.
    Precompute,
    /// Exact OLAP / time-series database — last resort for non-sketchable ops.
    Db,
}

impl std::fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PipelineStage::Agent => "agent",
            PipelineStage::Backend => "backend",
            PipelineStage::Precompute => "precompute",
            PipelineStage::Db => "db",
        };
        write!(f, "{s}")
    }
}

// ── Execution mode ────────────────────────────────────────────────────────────

/// Whether an operator uses sketch approximation or runs exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// The operator produces an approximate result via a data sketch.
    Sketch,
    /// The operator computes an exact result (no error bounds).
    Exact,
    /// The operator is a structural / routing node (merge, partition, …)
    /// that does not itself aggregate — its mode is determined by its children.
    Passthrough,
}

// ── Cost estimate ─────────────────────────────────────────────────────────────

/// Estimated resource cost of a single plan node.
///
/// The allocator fills this in using the same cost model as the legacy
/// [`crate::planner::cost_model`].  All fields default to `0.0` for nodes
/// whose cost is negligible or unknown.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Outbound bytes per second produced by this node.
    pub bytes_per_sec: f64,
    /// Memory footprint of the sketch or intermediate state (bytes).
    pub memory_bytes: f64,
    /// CPU overhead per input sample (µs).
    pub cpu_micros_per_sample: f64,
    /// Compression ratio relative to the raw OTLP baseline (≥ 1.0 is better).
    pub compression_ratio: f64,
}

// ── Node annotation ───────────────────────────────────────────────────────────

/// Extra hints attached to a plan node by the allocator.
///
/// Not all fields are relevant to all node types; unused fields are `None`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeAnnotation {
    /// Sketch type selected by the allocator (only for sketch nodes).
    pub sketch_type: Option<crate::types::SketchType>,
    /// Sketch parameters (width/depth/registers/epsilon).
    pub sketch_params: Option<crate::types::SketchParams>,
    /// Whether delta encoding should be used at this node.
    pub delta_enabled: bool,
    /// Minimum cell-change threshold for delta encoding (T).
    pub delta_threshold: f64,
    /// Human-readable explanation of why this stage/mode was chosen.
    pub rationale: String,
    /// Whether this node was demoted to a later stage due to budget overflow.
    pub budget_demotion: bool,
}

// ── Plan node ─────────────────────────────────────────────────────────────────

/// An annotated node in the physical execution plan.
///
/// The `expr` field holds the logical operator; the surrounding fields
/// describe where and how it runs.
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// The logical operator at this node.
    pub expr: QueryExpr,
    /// Which pipeline stage executes this operator.
    pub stage: PipelineStage,
    /// Sketch vs. exact vs. passthrough.
    pub mode: ExecutionMode,
    /// Estimated resource cost.
    pub cost: CostEstimate,
    /// Allocator hints for code-generation.
    pub annotation: NodeAnnotation,
    /// Child plan nodes (mirrors `expr`'s children after annotation).
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    /// Create a leaf `PlanNode` (no children) with default cost/annotation.
    pub fn leaf(expr: QueryExpr, stage: PipelineStage, mode: ExecutionMode) -> Self {
        Self {
            expr,
            stage,
            mode,
            cost: CostEstimate::default(),
            annotation: NodeAnnotation::default(),
            children: vec![],
        }
    }

    /// Recursively collect all nodes at a given stage, depth-first.
    pub fn nodes_at_stage(&self, target: &PipelineStage) -> Vec<&PlanNode> {
        let mut out = vec![];
        if &self.stage == target {
            out.push(self);
        }
        for c in &self.children {
            out.extend(c.nodes_at_stage(target));
        }
        out
    }

    /// Recursively collect all sketch nodes (mode == Sketch).
    pub fn sketch_nodes(&self) -> Vec<&PlanNode> {
        let mut out = vec![];
        if self.mode == ExecutionMode::Sketch {
            out.push(self);
        }
        for c in &self.children {
            out.extend(c.sketch_nodes());
        }
        out
    }

    /// Total estimated bandwidth of all nodes at `stage` (bytes/sec).
    pub fn stage_bandwidth(&self, stage: &PipelineStage) -> f64 {
        self.nodes_at_stage(stage)
            .iter()
            .map(|n| n.cost.bytes_per_sec)
            .sum()
    }

    /// Total estimated memory of all nodes at `stage` (bytes).
    pub fn stage_memory(&self, stage: &PipelineStage) -> f64 {
        self.nodes_at_stage(stage)
            .iter()
            .map(|n| n.cost.memory_bytes)
            .sum()
    }

    /// Returns a flat, depth-first list of `(depth, node)` pairs for display.
    pub fn flatten(&self) -> Vec<(usize, &PlanNode)> {
        let mut out = vec![];
        self.flatten_inner(0, &mut out);
        out
    }

    fn flatten_inner<'a>(&'a self, depth: usize, out: &mut Vec<(usize, &'a PlanNode)>) {
        out.push((depth, self));
        for c in &self.children {
            c.flatten_inner(depth + 1, out);
        }
    }
}

// ── Plan summary (serialisable) ───────────────────────────────────────────────

/// A serialisable summary of the full annotated plan, suitable for inclusion
/// in the `/api/v1/plan` JSON response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanSummary {
    /// Total estimated bandwidth saved vs raw OTLP (bytes/sec).
    pub bandwidth_saved_bytes_per_sec: f64,
    /// Total estimated agent memory for all sketch nodes (bytes).
    pub agent_memory_bytes: f64,
    /// Total estimated backend memory for all sketch nodes (bytes).
    pub backend_memory_bytes: f64,
    /// Whether any node was demoted due to budget overflow.
    pub has_budget_demotion: bool,
    /// List of per-node stage + mode + rationale entries.
    pub node_annotations: Vec<NodeSummaryEntry>,
}

/// One row in the [`PlanSummary::node_annotations`] table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSummaryEntry {
    pub node_kind: String,
    pub stage: PipelineStage,
    pub mode: ExecutionMode,
    pub rationale: String,
    pub memory_bytes: f64,
    pub bytes_per_sec: f64,
}

impl PlanNode {
    /// Build a [`PlanSummary`] from this root node.
    pub fn summarise(&self, raw_bytes_per_sec: f64) -> PlanSummary {
        let flat = self.flatten();
        let agent_mem: f64 = flat
            .iter()
            .filter(|(_, n)| n.stage == PipelineStage::Agent)
            .map(|(_, n)| n.cost.memory_bytes)
            .sum();
        let backend_mem: f64 = flat
            .iter()
            .filter(|(_, n)| n.stage == PipelineStage::Backend)
            .map(|(_, n)| n.cost.memory_bytes)
            .sum();
        let plan_bw: f64 = flat
            .iter()
            .filter(|(_, n)| matches!(n.stage, PipelineStage::Agent | PipelineStage::Backend))
            .map(|(_, n)| n.cost.bytes_per_sec)
            .fold(f64::INFINITY, f64::min); // min of outbound paths
        let saved = if raw_bytes_per_sec > plan_bw {
            raw_bytes_per_sec - plan_bw
        } else {
            0.0
        };
        let has_demotion = flat.iter().any(|(_, n)| n.annotation.budget_demotion);
        let entries = flat
            .iter()
            .map(|(_, n)| NodeSummaryEntry {
                node_kind: format!("{:?}", n.expr)
                    .split_whitespace()
                    .next()
                    .unwrap_or("?")
                    .to_string(),
                stage: n.stage.clone(),
                mode: n.mode.clone(),
                rationale: n.annotation.rationale.clone(),
                memory_bytes: n.cost.memory_bytes,
                bytes_per_sec: n.cost.bytes_per_sec,
            })
            .collect();
        PlanSummary {
            bandwidth_saved_bytes_per_sec: saved,
            agent_memory_bytes: agent_mem,
            backend_memory_bytes: backend_mem,
            has_budget_demotion: has_demotion,
            node_annotations: entries,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::QueryExpr;
    use crate::intent_algebra::legacy_expr::SourceSpec;

    fn source_node(name: &str, stage: PipelineStage) -> PlanNode {
        PlanNode::leaf(
            QueryExpr::Source(SourceSpec { name: name.into() }),
            stage,
            ExecutionMode::Passthrough,
        )
    }

    // ── PipelineStage ordering ────────────────────────────────────────────────

    #[test]
    fn stage_ordering_agent_lt_db() {
        assert!(PipelineStage::Agent < PipelineStage::Db);
        assert!(PipelineStage::Agent < PipelineStage::Backend);
        assert!(PipelineStage::Backend < PipelineStage::Precompute);
        assert!(PipelineStage::Precompute < PipelineStage::Db);
    }

    // ── nodes_at_stage ────────────────────────────────────────────────────────

    #[test]
    fn nodes_at_stage_collects_correctly() {
        let root = PlanNode {
            expr: QueryExpr::Source(SourceSpec {
                name: "root".into(),
            }),
            stage: PipelineStage::Agent,
            mode: ExecutionMode::Sketch,
            cost: CostEstimate {
                memory_bytes: 100.0,
                ..Default::default()
            },
            annotation: NodeAnnotation::default(),
            children: vec![
                source_node("child_agent", PipelineStage::Agent),
                source_node("child_backend", PipelineStage::Backend),
            ],
        };
        let agent_nodes = root.nodes_at_stage(&PipelineStage::Agent);
        assert_eq!(agent_nodes.len(), 2); // root + child_agent
        let backend_nodes = root.nodes_at_stage(&PipelineStage::Backend);
        assert_eq!(backend_nodes.len(), 1);
    }

    // ── sketch_nodes ─────────────────────────────────────────────────────────

    #[test]
    fn sketch_nodes_only_returns_sketch_mode() {
        let root = PlanNode {
            expr: QueryExpr::Source(SourceSpec { name: "r".into() }),
            stage: PipelineStage::Agent,
            mode: ExecutionMode::Sketch,
            cost: CostEstimate::default(),
            annotation: NodeAnnotation::default(),
            children: vec![
                PlanNode::leaf(
                    QueryExpr::Source(SourceSpec {
                        name: "exact_child".into(),
                    }),
                    PipelineStage::Db,
                    ExecutionMode::Exact,
                ),
                PlanNode::leaf(
                    QueryExpr::Source(SourceSpec {
                        name: "sketch_child".into(),
                    }),
                    PipelineStage::Backend,
                    ExecutionMode::Sketch,
                ),
            ],
        };
        let sn = root.sketch_nodes();
        assert_eq!(sn.len(), 2); // root (Sketch) + sketch_child
    }

    // ── stage_bandwidth / stage_memory ────────────────────────────────────────

    #[test]
    fn stage_bandwidth_sums_nodes_at_stage() {
        let root = PlanNode {
            expr: QueryExpr::Source(SourceSpec { name: "r".into() }),
            stage: PipelineStage::Agent,
            mode: ExecutionMode::Sketch,
            cost: CostEstimate {
                bytes_per_sec: 500.0,
                ..Default::default()
            },
            annotation: NodeAnnotation::default(),
            children: vec![PlanNode {
                expr: QueryExpr::Source(SourceSpec { name: "c".into() }),
                stage: PipelineStage::Agent,
                mode: ExecutionMode::Passthrough,
                cost: CostEstimate {
                    bytes_per_sec: 200.0,
                    ..Default::default()
                },
                annotation: NodeAnnotation::default(),
                children: vec![],
            }],
        };
        assert!((root.stage_bandwidth(&PipelineStage::Agent) - 700.0).abs() < 1e-6);
    }

    // ── flatten ───────────────────────────────────────────────────────────────

    #[test]
    fn flatten_returns_depth_zero_for_root() {
        let root = source_node("r", PipelineStage::Agent);
        let flat = root.flatten();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, 0); // depth = 0
    }

    #[test]
    fn flatten_depth_increments_per_level() {
        let root = PlanNode {
            expr: QueryExpr::Source(SourceSpec { name: "r".into() }),
            stage: PipelineStage::Agent,
            mode: ExecutionMode::Passthrough,
            cost: CostEstimate::default(),
            annotation: NodeAnnotation::default(),
            children: vec![source_node("c1", PipelineStage::Backend)],
        };
        let flat = root.flatten();
        assert_eq!(flat[0].0, 0);
        assert_eq!(flat[1].0, 1);
    }

    // ── PlanSummary ───────────────────────────────────────────────────────────

    #[test]
    fn summarise_reports_bandwidth_saved() {
        let root = PlanNode {
            expr: QueryExpr::Source(SourceSpec { name: "r".into() }),
            stage: PipelineStage::Agent,
            mode: ExecutionMode::Sketch,
            cost: CostEstimate {
                bytes_per_sec: 1_000.0,
                memory_bytes: 256.0,
                ..Default::default()
            },
            annotation: NodeAnnotation::default(),
            children: vec![],
        };
        // Raw baseline is 10 000 B/s; plan reduces to 1 000 B/s → saved = 9 000.
        let summary = root.summarise(10_000.0);
        assert!((summary.bandwidth_saved_bytes_per_sec - 9_000.0).abs() < 1.0);
        assert!((summary.agent_memory_bytes - 256.0).abs() < 1.0);
        assert!(!summary.has_budget_demotion);
    }

    #[test]
    fn summarise_detects_budget_demotion() {
        let root = PlanNode {
            expr: QueryExpr::Source(SourceSpec { name: "r".into() }),
            stage: PipelineStage::Backend,
            mode: ExecutionMode::Sketch,
            cost: CostEstimate::default(),
            annotation: NodeAnnotation {
                budget_demotion: true,
                ..Default::default()
            },
            children: vec![],
        };
        let summary = root.summarise(0.0);
        assert!(summary.has_budget_demotion);
    }
}
