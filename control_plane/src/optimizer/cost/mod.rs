use std::collections::HashMap;
use std::time::Duration;

// Sub-modules — formerly siblings of `planner/cost_model.rs` (now
// `optimizer/cost/mod.rs`); the 2026-05 refactor pulled each into a
// dedicated `optimizer/cost/<name>.rs`.
pub mod delta;
pub mod online;
pub mod pareto;
pub mod tco;
pub mod wire;

use self::delta::decide_delta;
use crate::optimizer::rules::{default_sketch_params, select_window_strategy, RulesPlanner};
use crate::types::*;

// ── Benchmark-derived cost table ──────────────────────────────────────────────
//
// Source: e2e benchmark results (2026-03-15), 1 000 series × 1 000 Hz row.
// Units: bandwidth bytes/series/sec, CPU µs/sample, memory bytes/sketch.

#[derive(Debug, Clone, Copy)]
pub struct SketchCosts {
    pub bytes_per_series_per_sec: f64,
    pub cpu_micros_per_sample: f64,
    pub base_memory_bytes: f64,
    pub relative_error_at_default: f64,
}

/// Public accessor used by `apply_delta_decision` to retrieve the cost table.
pub fn benchmark_table_pub() -> HashMap<SketchType, SketchCosts> {
    benchmark_table()
}

fn benchmark_table() -> HashMap<SketchType, SketchCosts> {
    [
        (
            SketchType::DDSketch,
            SketchCosts {
                bytes_per_series_per_sec: 120.0,
                cpu_micros_per_sample: 0.8,
                base_memory_bytes: 4_096.0,
                relative_error_at_default: 0.01,
            },
        ),
        (
            SketchType::KLL,
            SketchCosts {
                bytes_per_series_per_sec: 80.0,
                cpu_micros_per_sample: 0.5,
                base_memory_bytes: 2_048.0,
                relative_error_at_default: 0.02,
            },
        ),
        (
            SketchType::HLL,
            SketchCosts {
                bytes_per_series_per_sec: 40.0,
                cpu_micros_per_sample: 0.3,
                base_memory_bytes: 16_384.0, // precision=14 → 16 KB
                relative_error_at_default: 0.008,
            },
        ),
        (
            SketchType::CountSketch,
            SketchCosts {
                bytes_per_series_per_sec: 200.0,
                cpu_micros_per_sample: 1.2,
                base_memory_bytes: 40_960.0,
                relative_error_at_default: 0.01,
            },
        ),
        (
            SketchType::CountMinSketch,
            SketchCosts {
                bytes_per_series_per_sec: 200.0,
                cpu_micros_per_sample: 1.0,
                base_memory_bytes: 40_960.0,
                relative_error_at_default: 0.01,
            },
        ),
    ]
    .into()
}

// ── Scoring ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PlanScore {
    pub bandwidth_bytes_per_sec: f64,
    pub cpu_micros_per_sample: f64,
    pub memory_bytes: f64,
    pub estimated_error: f64,
    pub meets_sla: bool,
}

/// Estimates resource costs for a given plan + workload using the provided cost table.
pub fn score_with(
    plan: &CollectionPlan,
    w: &QueryWorkload,
    table: &HashMap<SketchType, SketchCosts>,
) -> PlanScore {
    let st = &plan.agent_config.sketch_type;
    let Some(&costs) = table.get(st) else {
        return PlanScore {
            bandwidth_bytes_per_sec: f64::MAX,
            cpu_micros_per_sample: f64::MAX,
            memory_bytes: f64::MAX,
            estimated_error: 1.0,
            meets_sla: false,
        };
    };

    let dim_multiplier = (plan.agent_config.aggregate_by.len() + 1) as f64;
    let bandwidth = costs.bytes_per_series_per_sec * dim_multiplier;
    let memory = costs.base_memory_bytes * dim_multiplier;
    let err = estimate_error(st, &plan.agent_config.sketch_params, costs);
    let sla = if w.accuracy_sla <= 0.0 {
        0.01
    } else {
        w.accuracy_sla
    };

    PlanScore {
        bandwidth_bytes_per_sec: bandwidth,
        cpu_micros_per_sample: costs.cpu_micros_per_sample,
        memory_bytes: memory,
        estimated_error: err,
        meets_sla: err <= sla,
    }
}

/// Estimates resource costs for a given plan + workload.
pub fn score(plan: &CollectionPlan, w: &QueryWorkload) -> PlanScore {
    let table = benchmark_table();
    let st = &plan.agent_config.sketch_type;

    let Some(&costs) = table.get(st) else {
        return PlanScore {
            bandwidth_bytes_per_sec: f64::MAX,
            cpu_micros_per_sample: f64::MAX,
            memory_bytes: f64::MAX,
            estimated_error: 1.0,
            meets_sla: false,
        };
    };

    // More preserved dimensions → more distinct sketches in flight.
    let dim_multiplier = (plan.agent_config.aggregate_by.len() + 1) as f64;

    let bandwidth = costs.bytes_per_series_per_sec * dim_multiplier;
    let memory = costs.base_memory_bytes * dim_multiplier;
    let err = estimate_error(st, &plan.agent_config.sketch_params, costs);

    let sla = if w.accuracy_sla <= 0.0 {
        0.01
    } else {
        w.accuracy_sla
    };

    PlanScore {
        bandwidth_bytes_per_sec: bandwidth,
        cpu_micros_per_sample: costs.cpu_micros_per_sample,
        memory_bytes: memory,
        estimated_error: err,
        meets_sla: err <= sla,
    }
}

fn estimate_error(_st: &SketchType, p: &SketchParams, costs: SketchCosts) -> f64 {
    match p {
        SketchParams::DDSketch {
            relative_accuracy, ..
        } if *relative_accuracy > 0.0 => *relative_accuracy,
        SketchParams::KLL { k, .. } if *k > 0 => 1.0 / *k as f64,
        SketchParams::HLL { precision } if *precision > 0 => {
            1.04 / (2.0f64.powi(*precision as i32)).sqrt()
        }
        _ => costs.relative_error_at_default,
    }
}

// ── CostModelPlanner ──────────────────────────────────────────────────────────

/// Extends the rule-based planner by scoring all valid sketch candidates and
/// choosing the one with the lowest bandwidth that still meets the AccuracySLA.
///
/// When an [`OnlineMetricsStore`] is attached (via [`CostModelPlanner::with_online_store`])
/// the planner blends live EMA observations into the cost table used for scoring,
/// so that real-world behaviour gradually supersedes the static benchmark defaults.
pub struct CostModelPlanner {
    inner: RulesPlanner,
    online_store: Option<online::OnlineMetricsStore>,
}

impl CostModelPlanner {
    pub fn new() -> Self {
        Self {
            inner: RulesPlanner::new(),
            online_store: None,
        }
    }

    pub fn with_sketch_defaults(mut self, defaults: SketchDefaults) -> Self {
        self.inner.sketch_defaults = defaults;
        self
    }

    /// Attach a live EMA store so scoring uses blended benchmark + observed costs.
    pub fn with_online_store(mut self, store: online::OnlineMetricsStore) -> Self {
        self.online_store = Some(store);
        self
    }

    /// Returns the effective cost table: online-blended when available, benchmark otherwise.
    fn cost_table(&self) -> HashMap<SketchType, SketchCosts> {
        match &self.online_store {
            Some(s) => online::effective_table(s),
            None => benchmark_table_pub(),
        }
    }

    /// Produces a [`CollectionPlan`] optimised for the given query workload
    /// and data characteristics.
    ///
    /// `wc` drives the delta transmission decision: fill rate, flush rate,
    /// CPU / memory overhead, and raw vs. sketch bandwidth comparison.
    /// Pass `None` to use conservative defaults (1 000 series, 100 Hz,
    /// 100 B/sample, Zipf distribution, no memory budget).
    pub fn plan(&self, w: &QueryWorkload, wc: Option<&WorkloadCharacteristics>) -> CollectionPlan {
        let default_wc;
        let wc = match wc {
            Some(c) => c,
            None => {
                default_wc = WorkloadCharacteristics::default();
                &default_wc
            }
        };

        let table = self.cost_table();

        // If a specific sketch type is pinned, use it directly.
        if let Some(st) = &w.sketch_type_override {
            let params = default_sketch_params(st, w.accuracy_sla);
            let (mode, window_duration) = select_window_strategy(w);
            let mut plan = self.inner.plan(w);
            plan.agent_config.sketch_type = st.clone();
            plan.agent_config.sketch_params = params;
            plan.agent_config.mode = mode;
            plan.agent_config.window_duration = window_duration;
            apply_delta_decision_with(&mut plan, w, wc, &table);
            return plan;
        }

        let candidates = crate::physical::sketch_catalog::candidates_for_workload(&w.aggregations);

        // Start with the rule-based plan as the baseline.
        let baseline = self.inner.plan(w);
        let mut best_plan = baseline;
        let mut best_score = score_with(&best_plan, w, &table);

        for st in candidates {
            let params = default_sketch_params(&st, w.accuracy_sla);
            let (mode, window_duration) = select_window_strategy(w);

            let mut trial = self.inner.plan(w);
            trial.agent_config.sketch_type = st.clone();
            trial.agent_config.sketch_params = params;
            trial.agent_config.mode = mode;
            trial.agent_config.window_duration = window_duration;

            let s = score_with(&trial, w, &table);
            if !s.meets_sla {
                continue;
            }

            if s.bandwidth_bytes_per_sec < best_score.bandwidth_bytes_per_sec
                || !best_score.meets_sla
            {
                best_plan = trial;
                best_score = s;
            }
        }

        apply_delta_decision_with(&mut best_plan, w, wc, &table);
        best_plan
    }
}

/// Runs the delta cost model and writes the decision into the plan using a provided cost table.
fn apply_delta_decision_with(
    plan: &mut CollectionPlan,
    w: &QueryWorkload,
    wc: &WorkloadCharacteristics,
    table: &HashMap<SketchType, SketchCosts>,
) {
    let bytes_per_series_per_sec = table
        .get(&plan.agent_config.sketch_type)
        .map(|c| c.bytes_per_series_per_sec)
        .unwrap_or(200.0);

    let (decision, summary) = decide_delta(plan, w, wc, bytes_per_series_per_sec);

    // Propagate into agent config.
    match &decision {
        DeltaDecision::UseDelta { threshold, .. } => {
            plan.agent_config.delta_transmission = true;
            plan.agent_config.delta_threshold = *threshold;
            // GOS relative delta gating: the edge replaces the fixed threshold
            // with the norm-adaptive one for Count-Sketch. ε_st = the staleness
            // share of the accuracy budget (w_edge=0 → all to thresholds).
            plan.agent_config.gos = (plan.agent_config.sketch_type == SketchType::CountSketch)
                .then(|| GosKnobs::derive(w.accuracy_sla, 1, 0.0, 1.0, false));
        }
        _ => {
            plan.agent_config.delta_transmission = false;
            plan.agent_config.delta_threshold = 0.0;
            plan.agent_config.gos = None;
        }
    }

    plan.delta_decision = decision;
    plan.transmission_cost_summary = summary;
}

// candidates_for_workload delegated to algebra::directory.

// ── Workload-level cost model (DAG fan-in shared-credit) ──────────────────────
//
// Phase F per `control_plane/docs/design.md` §6 `core::cost`. Single-query
// scoring above (`score`, `score_with`, `CostModelPlanner`) is the L4
// physical-plan cost; the workload-level entry point below is the L3
// shared-producer credit.
//
// Per design.md §6 the bundled cost of N related queries is *not* a sum
// of their individual costs — when q1 and q2 share a sub-DAG via a
// `LetBinding`/`Ref` pair, the build cost of that sub-DAG is paid once,
// not twice. The function below walks the L3 IR DAG (Phase B's
// `intent_algebra::QueryExpr`) post-order, memoises the binding-name
// space, and returns the bundled total along with how much was saved.
//
// This is the proof point that `Schema::unique_keys` from Phase B is
// load-bearing — the `cse_reuse_is_legal` gatekeeper in
// `intent_algebra::schema` is what allows the lower step to emit the
// `LetBinding` in the first place; the cost model collected here is
// what makes the planner *prefer* it.
//
// Phase F has no in-tree consumer of `workload_cost` yet — the analyzer
// wiring lands in a follow-up phase. `dead_code` is suppressed on the
// new surface (every item below carries `#[allow(dead_code)]`) to keep
// the lint baseline clean — mirrors the module-wide allowance on
// `intent_algebra/mod.rs` while Phase B sat consumer-less.
#[allow(unused_imports)]
use crate::intent_algebra::{AggIntent, BindingScope, QueryExpr, QueryExprError, Schema};
#[allow(unused_imports)]
use crate::types_v2::{BindingName, QueryId};

/// Bundled cost of a multi-query workload, with per-root contributions
/// and the savings unlocked by shared-producer credit. Returned by
/// [`workload_cost`].
///
/// `total_dollars` is the sum every consumer of `WorkloadCost` cares
/// about — the bundled plan beats N independent plans when this value
/// undercuts `Σ per_root_breakdown[i]` (no shared-producer credit). The
/// gap is exactly `reused_savings`, exposed for EXPLAIN / observability
/// per design.md §6 line ~1023.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadCost {
    /// Bundled total — each unique node costed once across all roots.
    /// Units: abstract "dollars" (the same scalar L4's `dollars(plan)`
    /// returns; cost-model agnostic so the same number composes with
    /// the L4 cost model when the planner gains an L3+L4 stack).
    pub total_dollars: f64,
    /// Per-root contribution if that root were the sole consumer (i.e.
    /// what the naive sum-over-roots cost model would charge it). The
    /// difference between `Σ per_root_breakdown[i].1` and `total_dollars`
    /// is exactly `reused_savings`.
    pub per_root_breakdown: Vec<(QueryId, f64)>,
    /// Savings from shared-producer credit vs. naive sum-over-roots.
    /// Always `>= 0.0`; equals `0.0` when no `LetBinding` is referenced
    /// from more than one consumer (the degenerate "no reuse" case).
    pub reused_savings: f64,
}

/// Multi-root container the cost model walks. Mirrors the shape of
/// `types_v2::WorkloadPlan` (`bindings + roots`) but uses the real
/// Phase B `QueryExpr` instead of the `QueryExprPlaceholder` string.
///
/// Lives here rather than in `types_v2` because `types_v2::WorkloadPlan`
/// is the JSON wire shape (string placeholder for QueryExpr until the
/// L3 IR gets a stable serde shape downstream); the cost model needs
/// the live IR. When `types_v2::WorkloadPlan` swaps the placeholder for
/// `QueryExpr`, this struct can be replaced with that one and
/// [`workload_cost`] re-pointed without an API break.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkloadCostPlan<'a> {
    /// Named shared producers. Each binding is referenced by ≥2 roots
    /// via `QueryExpr::Ref` for the cost model to credit it as reused.
    pub bindings: Vec<(BindingName, &'a QueryExpr)>,
    /// One root per `QuerySpec`, in input order.
    pub roots: Vec<(QueryId, &'a QueryExpr)>,
}

/// Cost a multi-query workload plan, crediting shared producers
/// (`LetBinding` referenced by ≥2 `Ref`s) ONCE across consumers.
///
/// Per design.md §6 batched-queries example (line ~1256): when q1 + q2
/// share a `Window` producer hoisted into a `WorkloadCostPlan::bindings`
/// entry, the Scan + Window build cost is credited once across the
/// workload, not twice. This is what makes the bundled plan beat 2
/// independent plans on `total_dollars`.
///
/// The walk is post-order with memoisation:
/// 1. Each `bindings[i]` entry is costed exactly once and registered
///    in a binding-name → cost map.
/// 2. Each root walks its tree; encountering `Ref(name)` adds 0.0
///    (the binding's cost has already been paid).
/// 3. The `total_dollars` is `Σ binding_costs + Σ per_root_traversal`
///    (refs charge zero, so shared work is paid once).
/// 4. `per_root_breakdown` reports each root as if it were the sole
///    consumer (full subtree cost) so the savings are visible.
///
/// Errors from `QueryExpr` (unresolved `Ref`, invalid by-column,
/// `Window` missing time index) propagate via [`QueryExprError`].
#[allow(dead_code)]
pub fn workload_cost(plan: &WorkloadCostPlan<'_>) -> Result<WorkloadCost, QueryExprError> {
    // 1. Cost each binding once. Bindings shadow earlier bindings in
    //    forward order (mirrors `LetBinding`'s lexical-scope semantics).
    let mut binding_costs: HashMap<String, f64> = HashMap::new();
    let mut schema_scope = BindingScope::new();
    for (name, expr) in &plan.bindings {
        let cost = subtree_cost(expr, &binding_costs, &schema_scope)?;
        let schema = expr.output_schema_in(&schema_scope)?;
        binding_costs.insert(name.as_str().to_owned(), cost);
        schema_scope = schema_scope.with(name.clone(), schema);
    }

    // 2. Cost each root in the bindings scope. `Ref` lookups charge 0.0
    //    (the binding has already been paid for above). `LetBinding`
    //    nodes inside a root are within-query CTE fan-in (design.md §6
    //    line ~1318) — same memoisation logic, costed once per root.
    let mut bundled_root_total = 0.0;
    let mut per_root_breakdown: Vec<(QueryId, f64)> = Vec::with_capacity(plan.roots.len());
    for (qid, root) in &plan.roots {
        // "What the bundled plan charges this root" — refs and
        // workload-level bindings are free here.
        let bundled_contribution = subtree_cost_bundled(root, &binding_costs, &schema_scope)?;
        bundled_root_total += bundled_contribution;
        // "What this root would cost if it owned the whole sub-DAG"
        // — every binding it references is paid for in full. Used
        // only for breakdown reporting; the bundled total above is
        // the actual cost.
        let standalone = subtree_cost_standalone(root, &binding_costs, &schema_scope)?;
        per_root_breakdown.push((qid.clone(), standalone));
    }

    // 3. Bundled total = sum of binding costs (each paid once) +
    //    bundled per-root contributions (refs charged zero above).
    let bindings_total: f64 = binding_costs.values().sum();
    let total_dollars = bindings_total + bundled_root_total;

    // 4. Savings = naive-sum total − bundled total. Naive sum is
    //    `Σ per_root_breakdown.1` (each root pays for everything it
    //    transitively references, double-counting shared producers).
    let naive_sum: f64 = per_root_breakdown.iter().map(|(_, c)| *c).sum();
    let reused_savings = (naive_sum - total_dollars).max(0.0);

    Ok(WorkloadCost {
        total_dollars,
        per_root_breakdown,
        reused_savings,
    })
}

/// Cost of a single subtree, treating `Ref(name)` as a free pointer to
/// the already-paid binding. Used for the bundled per-root contribution.
#[allow(dead_code)]
fn subtree_cost_bundled(
    expr: &QueryExpr,
    binding_costs: &HashMap<String, f64>,
    schema_scope: &BindingScope,
) -> Result<f64, QueryExprError> {
    match expr {
        QueryExpr::Ref { name } => {
            // Ref is free — the binding has been paid for at the workload
            // level. Resolve to assert the name is bound (otherwise the
            // walk should error rather than silently zero-cost an
            // unresolved reference).
            if binding_costs.contains_key(name.as_str()) {
                Ok(0.0)
            } else {
                Err(QueryExprError::UnresolvedRef(name.as_str().into()))
            }
        }
        QueryExpr::Scan { schema, .. } => Ok(node_cost_scan(schema)),
        QueryExpr::Window { child, .. } => {
            let cs = subtree_cost_bundled(child, binding_costs, schema_scope)?;
            let in_schema = child.output_schema_in(schema_scope)?;
            Ok(node_cost_window(&in_schema) + cs)
        }
        QueryExpr::Aggregate {
            by, aggs, child, ..
        } => {
            let cs = subtree_cost_bundled(child, binding_costs, schema_scope)?;
            let in_schema = child.output_schema_in(schema_scope)?;
            Ok(node_cost_aggregate(by, aggs, &in_schema) + cs)
        }
        QueryExpr::LetBinding { name, expr, child } => {
            // Within-query LetBinding — same shared-credit logic. Cost
            // `expr` once, expose it under `name`, then walk `child`.
            let mut extended = binding_costs.clone();
            let bind_cost = subtree_cost(expr, binding_costs, schema_scope)?;
            let bound_schema = expr.output_schema_in(schema_scope)?;
            let extended_scope = schema_scope.with(name.clone(), bound_schema);
            extended.insert(name.as_str().to_owned(), bind_cost);
            let child_cost = subtree_cost_bundled(child, &extended, &extended_scope)?;
            Ok(bind_cost + child_cost)
        }
        // A-variants lifted in Batch 2 of the relational migration — no
        // canonical-cost consumer exercises them yet. Fall through to a
        // child-walk-only contribution (0 cost added at this node) until
        // the per-node cost primitives land alongside their consumers.
        // TODO(relational-migration): add proper node_cost_* helpers for
        // Filter/Project/Partition/Distinct/Merge/Join/SetOp/Sort/Limit/BinaryOp.
        _ => walk_children_zero_cost_bundled(expr, binding_costs, schema_scope),
    }
}

/// Per Batch 2 — walks A-variant children for cost without charging the
/// node itself. Drops out cleanly when those variants gain proper cost
/// primitives. Placement here (rather than inline) keeps the main match
/// arms readable.
#[allow(dead_code)]
fn walk_children_zero_cost_bundled(
    expr: &QueryExpr,
    binding_costs: &HashMap<String, f64>,
    schema_scope: &BindingScope,
) -> Result<f64, QueryExprError> {
    match expr {
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Partition { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. } => {
            subtree_cost_bundled(child, binding_costs, schema_scope)
        }
        QueryExpr::Merge { children } => children
            .iter()
            .map(|c| subtree_cost_bundled(c, binding_costs, schema_scope))
            .sum(),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => {
            let l = subtree_cost_bundled(left, binding_costs, schema_scope)?;
            let r = subtree_cost_bundled(right, binding_costs, schema_scope)?;
            Ok(l + r)
        }
        _ => Ok(0.0),
    }
}

/// Cost of a single subtree as if it owned every reference it transitively
/// makes (refs charge their full binding cost). Used to compute the
/// per-root breakdown — what each query would cost if it were the sole
/// consumer.
#[allow(dead_code)]
fn subtree_cost_standalone(
    expr: &QueryExpr,
    binding_costs: &HashMap<String, f64>,
    schema_scope: &BindingScope,
) -> Result<f64, QueryExprError> {
    match expr {
        QueryExpr::Ref { name } => binding_costs
            .get(name.as_str())
            .copied()
            .ok_or_else(|| QueryExprError::UnresolvedRef(name.as_str().into())),
        QueryExpr::Scan { schema, .. } => Ok(node_cost_scan(schema)),
        QueryExpr::Window { child, .. } => {
            let cs = subtree_cost_standalone(child, binding_costs, schema_scope)?;
            let in_schema = child.output_schema_in(schema_scope)?;
            Ok(node_cost_window(&in_schema) + cs)
        }
        QueryExpr::Aggregate {
            by, aggs, child, ..
        } => {
            let cs = subtree_cost_standalone(child, binding_costs, schema_scope)?;
            let in_schema = child.output_schema_in(schema_scope)?;
            Ok(node_cost_aggregate(by, aggs, &in_schema) + cs)
        }
        QueryExpr::LetBinding { name, expr, child } => {
            let mut extended = binding_costs.clone();
            let bind_cost = subtree_cost(expr, binding_costs, schema_scope)?;
            let bound_schema = expr.output_schema_in(schema_scope)?;
            let extended_scope = schema_scope.with(name.clone(), bound_schema);
            extended.insert(name.as_str().to_owned(), bind_cost);
            let child_cost = subtree_cost_standalone(child, &extended, &extended_scope)?;
            Ok(bind_cost + child_cost)
        }
        // See Batch-2 note on subtree_cost_bundled — same story here.
        _ => walk_children_zero_cost_standalone(expr, binding_costs, schema_scope),
    }
}

#[allow(dead_code)]
fn walk_children_zero_cost_standalone(
    expr: &QueryExpr,
    binding_costs: &HashMap<String, f64>,
    schema_scope: &BindingScope,
) -> Result<f64, QueryExprError> {
    match expr {
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Partition { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. } => {
            subtree_cost_standalone(child, binding_costs, schema_scope)
        }
        QueryExpr::Merge { children } => children
            .iter()
            .map(|c| subtree_cost_standalone(c, binding_costs, schema_scope))
            .sum(),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => {
            let l = subtree_cost_standalone(left, binding_costs, schema_scope)?;
            let r = subtree_cost_standalone(right, binding_costs, schema_scope)?;
            Ok(l + r)
        }
        _ => Ok(0.0),
    }
}

/// Cost a sub-expression in isolation (no fan-in credit). Used by
/// `workload_cost` to price each binding's body, where there's no
/// outer `LetBinding`/`Ref` semantics to honour.
#[allow(dead_code)]
fn subtree_cost(
    expr: &QueryExpr,
    binding_costs: &HashMap<String, f64>,
    schema_scope: &BindingScope,
) -> Result<f64, QueryExprError> {
    // Same shape as standalone — bindings are walked in their own
    // scope where outer bindings are visible (workload-level bindings
    // can reference earlier ones).
    subtree_cost_standalone(expr, binding_costs, schema_scope)
}

// ── Per-node cost primitives ─────────────────────────────────────────────────
//
// Numbers are in the same abstract-dollars unit as L4's `dollars(plan)`
// (`design.md` §6 `core::cost`). Calibration vs. real benchmarks is
// future work — what matters for Phase F is that:
// - costs are positive,
// - costs are monotonic in input size (Schema width as a proxy),
// - costs compose additively over sub-trees,
// so the test suite can assert "shared producer credited once" without
// committing to a specific magnitude.

/// Cost of a `Scan` — proportional to the number of columns scanned.
/// The scan dominates I/O in single-query plans; making it cheap here
/// would make shared-producer credit invisible. 10.0 dollars per column
/// is the placeholder; calibration is downstream.
#[allow(dead_code)]
fn node_cost_scan(schema: &Schema) -> f64 {
    10.0 * schema.columns.len().max(1) as f64
}

/// Cost of a `Window` — proportional to the input row count, here proxied
/// by the input schema width. 2.0 per column reflects that windowing is
/// cheap relative to a scan but not free (state buffer per row).
#[allow(dead_code)]
fn node_cost_window(input: &Schema) -> f64 {
    2.0 * input.columns.len().max(1) as f64
}

/// Cost of an `Aggregate { by, aggs }` — `|by| * 1.0 + Σ cost(intent)`.
/// Each intent contributes its own cost; sketch-bound intents will be
/// re-priced in L4 via the existing `score_with` path. At L3 the only
/// signal is the intent vocabulary, which is a coarse-but-monotonic
/// proxy.
#[allow(dead_code)]
fn node_cost_aggregate(by: &[usize], aggs: &[AggIntent], _input: &Schema) -> f64 {
    let by_cost = by.len() as f64;
    let agg_cost: f64 = aggs.iter().map(intent_cost).sum();
    by_cost + agg_cost
}

/// Cost of a single `AggIntent`. Sketch-amenable intents that L4 will
/// later re-price (`Quantile`, `Cardinality`, `TopK`, `Frequency`,
/// `Count{Epsilon}`) are marked more expensive so the cost model
/// agrees with L4's "sketch is cheaper than exact for these" intuition.
#[allow(dead_code)]
fn intent_cost(intent: &AggIntent) -> f64 {
    match intent {
        AggIntent::Sum | AggIntent::Min | AggIntent::Max | AggIntent::Avg => 5.0,
        AggIntent::Count { .. } => 5.0,
        AggIntent::Quantile { .. } => 20.0,
        AggIntent::Cardinality { .. } => 15.0,
        AggIntent::TopK { .. } => 25.0,
        AggIntent::Frequency { .. } => 15.0,
        AggIntent::Rate { .. } | AggIntent::Increase { .. } => 8.0,
        // Phase β archive-only intents — priced as a cold-tier scan
        // rather than a streaming aggregate. Higher than `Sum` (the engine
        // must read the raw archive) but lower than the sketch intents
        // (no per-sample sketch update on the hot path). Tightening this
        // is a follow-up once real measurements land.
        intent if intent.archive_only() => 12.0,
        // Defensive fallback — any future intent that isn't archive-only
        // and doesn't match an explicit arm prices as a generic aggregate.
        _ => 5.0,
    }
}

// ── Workload-cost tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod workload_cost_tests {
    use super::*;
    use crate::intent_algebra::{
        AggIntent, Column, DataType, LabelFilter, QueryExpr, Schema, Source, WindowKind,
    };
    use crate::types_v2::{AccuracyTarget, BindingName, QueryId};
    use std::time::Duration;

    fn col(name: &str, dtype: DataType) -> Column {
        Column {
            name: name.into(),
            dtype,
            nullable: false,
        }
    }

    fn ts_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "http_request_duration_seconds".into(),
            },
            label_filters: vec![LabelFilter {
                label: "service".into(),
                equals: "api".into(),
            }],
            schema: Schema::with_time_index(
                vec![
                    col("ts", DataType::Timestamp),
                    col("service", DataType::Utf8),
                    col("value", DataType::Float64),
                ],
                0,
                vec![vec![0, 1]],
            ),
        }
    }

    fn windowed_scan() -> QueryExpr {
        QueryExpr::Window {
            kind: WindowKind::Sliding,
            size: Duration::from_secs(300),
            slide: None,
            child: Box::new(ts_scan()),
        }
    }

    /// Wrap `child` in `Aggregate { by: [], aggs: [Quantile{q}] }`.
    fn quantile_root(q: f64, child: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![AggIntent::Quantile {
                q,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(child),
        }
    }

    /// Wrap `child` in `Aggregate { by: [], aggs: [Max] }`.
    fn max_root(child: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![AggIntent::Max],
            having: None,
            child: Box::new(child),
        }
    }

    /// Single-root degenerate case — `workload_cost` for one query
    /// equals the standalone cost of that query (no reuse possible).
    #[test]
    fn workload_cost_single_root_equals_query_cost() {
        let q = quantile_root(0.99, windowed_scan());
        let plan = WorkloadCostPlan {
            bindings: vec![],
            roots: vec![(QueryId::new("q1"), &q)],
        };
        let wc = workload_cost(&plan).unwrap();

        // Per-root breakdown reports the standalone cost of `q`.
        let standalone =
            subtree_cost_standalone(&q, &HashMap::new(), &BindingScope::new()).unwrap();
        assert_eq!(wc.per_root_breakdown.len(), 1);
        assert_eq!(wc.per_root_breakdown[0].0, QueryId::new("q1"));
        assert!((wc.per_root_breakdown[0].1 - standalone).abs() < 1e-9);
        // Total equals standalone (no bindings, single root).
        assert!((wc.total_dollars - standalone).abs() < 1e-9);
        // No sharing → no savings.
        assert!(wc.reused_savings.abs() < 1e-9);
    }

    /// Two roots that share NOTHING — bundled cost equals sum of
    /// standalone costs. `reused_savings` is zero.
    #[test]
    fn workload_cost_two_roots_no_sharing_equals_sum() {
        let q1 = quantile_root(0.99, windowed_scan());
        let q2 = max_root(windowed_scan());
        let plan = WorkloadCostPlan {
            bindings: vec![],
            roots: vec![(QueryId::new("q1"), &q1), (QueryId::new("q2"), &q2)],
        };
        let wc = workload_cost(&plan).unwrap();

        let s1 = subtree_cost_standalone(&q1, &HashMap::new(), &BindingScope::new()).unwrap();
        let s2 = subtree_cost_standalone(&q2, &HashMap::new(), &BindingScope::new()).unwrap();
        assert!((wc.total_dollars - (s1 + s2)).abs() < 1e-9);
        assert!(wc.reused_savings.abs() < 1e-9);
        assert_eq!(wc.per_root_breakdown.len(), 2);
    }

    /// design.md §6 batched-queries example: q1 (Quantile{0.99}) and
    /// q2 (Quantile{0.95}) share a `Window` producer hoisted into a
    /// workload-level binding. The Scan + Window cost is credited once,
    /// not twice — `reused_savings > 0`, and `total_dollars` undercuts
    /// the naive sum-over-roots.
    #[test]
    fn workload_cost_two_roots_shared_window_credits_once() {
        let shared = windowed_scan();
        let q1 = quantile_root(
            0.99,
            QueryExpr::Ref {
                name: BindingName::new("w"),
            },
        );
        let q2 = quantile_root(
            0.95,
            QueryExpr::Ref {
                name: BindingName::new("w"),
            },
        );

        let plan = WorkloadCostPlan {
            bindings: vec![(BindingName::new("w"), &shared)],
            roots: vec![(QueryId::new("q1"), &q1), (QueryId::new("q2"), &q2)],
        };
        let wc = workload_cost(&plan).unwrap();

        // The shared-producer cost contribution.
        let shared_cost = subtree_cost(&shared, &HashMap::new(), &BindingScope::new()).unwrap();
        // Naive sum-over-roots = each root pays for its full subtree
        // including the shared sub-DAG.
        let naive_sum: f64 = wc.per_root_breakdown.iter().map(|(_, c)| *c).sum();

        // Bundled total < naive sum by exactly one shared_cost (paid
        // once vs. twice).
        assert!(
            wc.total_dollars < naive_sum,
            "bundled ({}) must beat naive ({}) when a producer is shared",
            wc.total_dollars,
            naive_sum
        );
        // Savings ≈ shared_cost (the producer paid once instead of
        // twice).
        assert!(
            (wc.reused_savings - shared_cost).abs() < 1e-9,
            "expected savings ≈ shared_cost ({}); got {}",
            shared_cost,
            wc.reused_savings
        );
        assert_eq!(wc.per_root_breakdown.len(), 2);
    }

    /// q1 + q2 share a binding; q3 is independent. Savings credit only
    /// the (n_consumers − 1) × binding_cost for the shared portion.
    #[test]
    fn workload_cost_three_roots_two_share_partial() {
        let shared = windowed_scan();
        let q1 = quantile_root(
            0.99,
            QueryExpr::Ref {
                name: BindingName::new("w"),
            },
        );
        let q2 = quantile_root(
            0.95,
            QueryExpr::Ref {
                name: BindingName::new("w"),
            },
        );
        // q3 builds its own scan + window — no shared producer.
        let q3 = max_root(windowed_scan());

        let plan = WorkloadCostPlan {
            bindings: vec![(BindingName::new("w"), &shared)],
            roots: vec![
                (QueryId::new("q1"), &q1),
                (QueryId::new("q2"), &q2),
                (QueryId::new("q3"), &q3),
            ],
        };
        let wc = workload_cost(&plan).unwrap();

        let shared_cost = subtree_cost(&shared, &HashMap::new(), &BindingScope::new()).unwrap();
        // Two consumers of `w` ⇒ one duplicated copy avoided ⇒
        // savings ≈ shared_cost (not 2 × shared_cost).
        assert!(
            (wc.reused_savings - shared_cost).abs() < 1e-9,
            "two consumers should save 1× shared_cost ({}); got {}",
            shared_cost,
            wc.reused_savings
        );
        assert_eq!(wc.per_root_breakdown.len(), 3);
    }

    /// All three roots share the same binding — savings = 2 × shared_cost
    /// (3 consumers, 1 paid, 2 avoided). The "fan-in across {q1, q2, q3}"
    /// case design.md §6 line ~1318 calls out by name.
    #[test]
    fn workload_cost_three_roots_all_share_one_binding() {
        let shared = windowed_scan();
        let q1 = quantile_root(
            0.99,
            QueryExpr::Ref {
                name: BindingName::new("w"),
            },
        );
        let q2 = quantile_root(
            0.95,
            QueryExpr::Ref {
                name: BindingName::new("w"),
            },
        );
        let q3 = max_root(QueryExpr::Ref {
            name: BindingName::new("w"),
        });
        let plan = WorkloadCostPlan {
            bindings: vec![(BindingName::new("w"), &shared)],
            roots: vec![
                (QueryId::new("q1"), &q1),
                (QueryId::new("q2"), &q2),
                (QueryId::new("q3"), &q3),
            ],
        };
        let wc = workload_cost(&plan).unwrap();
        let shared_cost = subtree_cost(&shared, &HashMap::new(), &BindingScope::new()).unwrap();
        assert!(
            (wc.reused_savings - 2.0 * shared_cost).abs() < 1e-9,
            "three consumers should save 2× shared_cost ({}); got {}",
            shared_cost,
            wc.reused_savings
        );
    }

    /// Savings is informational and never negative even when the
    /// bindings include unused entries (defensive — a binding with no
    /// `Ref` consumer means the bundled total includes the binding
    /// once and the naive sum *also* counts it once, so savings is 0).
    #[test]
    fn workload_cost_unused_binding_is_zero_savings_not_negative() {
        let unused = windowed_scan();
        let q1 = quantile_root(0.99, windowed_scan());
        let plan = WorkloadCostPlan {
            bindings: vec![(BindingName::new("unused"), &unused)],
            roots: vec![(QueryId::new("q1"), &q1)],
        };
        let wc = workload_cost(&plan).unwrap();
        assert!(
            wc.reused_savings >= 0.0,
            "savings must never go negative; got {}",
            wc.reused_savings
        );
    }

    /// `Ref` to an undeclared binding errors rather than silently
    /// zero-costing — caught at workload-cost time so the planner
    /// can refuse the plan rather than under-quote it.
    #[test]
    fn workload_cost_unresolved_ref_errors() {
        let q = quantile_root(
            0.99,
            QueryExpr::Ref {
                name: BindingName::new("missing"),
            },
        );
        let plan = WorkloadCostPlan {
            bindings: vec![],
            roots: vec![(QueryId::new("q1"), &q)],
        };
        let err = workload_cost(&plan).unwrap_err();
        assert!(matches!(err, QueryExprError::UnresolvedRef(s) if s == "missing"));
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    fn workload(aggs: Vec<AggType>) -> QueryWorkload {
        QueryWorkload {
            metric_name: "test".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: aggs,
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    fn dummy_plan(st: SketchType) -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: st.clone(),
                sketch_params: default_sketch_params(&st, 0.01),
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: Some(Duration::from_secs(300)),
                mode: ProcessorMode::Window,
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: false,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            precompute: vec![],
            valid_until: Utc::now(),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    #[test]
    fn ddsketch_meets_sla_at_1pct() {
        let w = workload(vec![AggType::Quantile]);
        let s = score(&dummy_plan(SketchType::DDSketch), &w);
        assert!(
            s.meets_sla,
            "DDSketch at 1% should meet 1% SLA, error={}",
            s.estimated_error
        );
    }

    #[test]
    fn ddsketch_fails_tight_sla() {
        let w = QueryWorkload {
            accuracy_sla: 0.001,
            ..workload(vec![AggType::Quantile])
        };
        // Force 1% params despite tighter SLA.
        let mut plan = dummy_plan(SketchType::DDSketch);
        plan.agent_config.sketch_params = SketchParams::DDSketch {
            relative_accuracy: 0.01,
            quantiles: vec![0.5, 0.99],
        };
        let s = score(&plan, &w);
        assert!(!s.meets_sla, "DDSketch at 1% should NOT meet 0.1% SLA");
    }

    #[test]
    fn hll_lower_bandwidth_than_ddsketch() {
        let w = workload(vec![AggType::Quantile]);
        let s_dd = score(&dummy_plan(SketchType::DDSketch), &w);
        let s_hll = score(&dummy_plan(SketchType::HLL), &w);
        assert!(s_hll.bandwidth_bytes_per_sec < s_dd.bandwidth_bytes_per_sec);
    }

    #[test]
    fn dim_multiplier_increases_bandwidth() {
        let w_few = QueryWorkload {
            group_by_labels: vec!["host".into()],
            ..workload(vec![AggType::Quantile])
        };
        let w_many = QueryWorkload {
            group_by_labels: vec![
                "host".into(),
                "service".into(),
                "zone".into(),
                "region".into(),
            ],
            ..workload(vec![AggType::Quantile])
        };
        let pl = RulesPlanner::new();
        let s_few = score(&pl.plan(&w_few), &w_few);
        let s_many = score(&pl.plan(&w_many), &w_many);
        assert!(s_many.bandwidth_bytes_per_sec > s_few.bandwidth_bytes_per_sec);
    }

    #[test]
    fn kll_error_formula() {
        let w = QueryWorkload {
            accuracy_sla: 0.02,
            ..workload(vec![AggType::Quantile])
        };
        let mut plan = dummy_plan(SketchType::KLL);
        plan.agent_config.sketch_params = SketchParams::KLL {
            k: 100, // error ≈ 1/100 = 1%
            quantiles: vec![0.5, 0.99],
        };
        let s = score(&plan, &w);
        assert!(s.meets_sla, "KLL k=100 (error~1%) should meet 2% SLA");
    }

    #[test]
    fn cost_model_planner_meets_sla_for_all_agg_types() {
        let pl = CostModelPlanner::new();
        for (agg, sla) in [
            (AggType::Quantile, 0.01),
            (AggType::Cardinality, 0.01),
            (AggType::Frequency, 0.02),
        ] {
            let w = QueryWorkload {
                accuracy_sla: sla,
                ..workload(vec![agg])
            };
            let plan = pl.plan(&w, None);
            let s = score(&plan, &w);
            assert!(
                s.meets_sla,
                "agg={} sla={sla}: plan does not meet SLA (error={})",
                w.aggregations[0], s.estimated_error
            );
        }
    }

    #[test]
    fn cost_model_prefers_lower_bandwidth_for_cardinality() {
        let w = QueryWorkload {
            accuracy_sla: 0.02,
            ..workload(vec![AggType::Cardinality])
        };
        let plan = CostModelPlanner::new().plan(&w, None);
        assert_eq!(
            plan.agent_config.sketch_type,
            SketchType::HLL,
            "HLL should win for cardinality (lowest bandwidth)"
        );
    }

    #[test]
    fn cost_model_valid_until_in_future() {
        let plan = CostModelPlanner::new().plan(&workload(vec![AggType::Quantile]), None);
        assert!(plan.valid_until > Utc::now());
    }
}
