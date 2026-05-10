//! SP-9: AST-aware hierarchical stage assignment.
//!
//! Splits an optimised [`QueryExpr`] tree across pipeline stages, emitting a
//! [`StagedPlan`] that carries per-stage sub-plans for:
//!
//! | Stage | Nodes |
//! |---|---|
//! | Agent OTel Collector | `Source`, `Filter`, `Window`, `SketchAgg` (sketch ops) |
//! | Backend OTel Collector | `Partition`, `Merge`, `Dedup`, `Aggregate { Exact(Sum\|Count\|Min\|Max) }` |
//! | ASAPQuery Precompute Engine | `TopK`, `HistogramQuantile`, `PromQLSubquery`, deferred sketch ops |
//! | DB-side query | `Aggregate { Avg }` (non-mergeable) |
//!
//! # ExactAgg / AggFunc deferral
//!
//! Mergeability drives `Exact` op placement:
//! - `Sum`, `Count`, `Min`, `Max` — mergeable (`agg(A∪B) = merge(agg(A), agg(B))`) →
//!   **Backend** (the backend collector can combine partial results from N agents).
//! - `Avg`, `StdDev`, `Variance` — **not** mergeable → **DB-side** query.
//!
//! # Budget-driven deferral chain
//!
//! When a `SketchAgg` node's estimated memory cost exceeds the stage cap in
//! [`StageResourceBudgets`], it is deferred to the next stage:
//!
//! `Agent → Backend → Precompute`
//!
//! The degenerate-but-valid fallback (all sketch ops deferred to Precompute)
//! matches the current flat SP-3 behaviour and ensures the query is always
//! answerable.
//!
//! # PromQL serialisation
//!
//! [`expr_to_promql`] converts a [`QueryExpr`] tree to a valid PromQL expression
//! consumed by the ASAPQuery Precompute Engine's query engine.  Unlike the old
//! flat-template approach, this uses a proper recursive descent so it handles
//! `histogram_quantile`, `PromQLSubquery`, and vector `BinaryOp` nodes natively.

use std::time::Duration;

use crate::algebra::expr::{AggFunc, BinaryOpKind, LiteralValue, QueryExpr, ScalarExpr};
use crate::analyzer::format_duration;
use crate::algebra::expr::{AggIntent, ExactAgg};
use crate::algebra::directory;
use crate::types::{
    AgentSubPlan, BackendSubPlan,
    DbSubPlan, PrecomputeSubPlan, SketchParams, SketchType,
    StagedPlan, StageResourceBudgets,
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Split a [`QueryExpr`] tree across pipeline stages, respecting per-stage
/// resource budgets and the `AggFunc` mergeability rules described above.
///
/// The returned [`StagedPlan`] is attached to
/// [`crate::types::CollectionPlan::staged_plan`] by the caller
/// (`handle_plan` in `main.rs`).
pub fn split_expr_by_stage(expr: &QueryExpr, budgets: &StageResourceBudgets) -> StagedPlan {
    let mut plan = StagedPlan::default();
    walk(expr, &mut plan, budgets);

    // Build the precompute query_expr from the full tree when the precompute
    // stage is active (TopK, HistogramQuantile, PromQLSubquery, or deferred ops).
    if plan.precompute.active && plan.precompute.query_expr.is_empty() {
        plan.precompute.query_expr = expr_to_promql(expr);
    }

    // Build the DB query_expr when a non-mergeable agg was assigned to DB stage.
    if plan.db.active && plan.db.query_expr.is_empty() {
        plan.db.query_expr = expr_to_promql(expr);
    }

    plan
}

/// Serialise a [`QueryExpr`] tree to a valid PromQL expression string.
///
/// The output is consumed by the ASAPQuery Precompute Engine's query engine.
/// Sketch data is already ingested from the Backend OTel Collector; the PromQL
/// describes the aggregation to apply over it.
///
/// Uses recursive descent, so it handles `histogram_quantile`,
/// `PromQLSubquery`, and vector `BinaryOp` nodes that the old flat-template
/// could not represent.
pub fn expr_to_promql(expr: &QueryExpr) -> String {
    let mut ctx = PromQLCtx::default();
    promql_from_qe(expr, &mut ctx)
}

// ── Tree walker ───────────────────────────────────────────────────────────────

fn walk(expr: &QueryExpr, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    match expr {
        // Source — always Agent; populate metric name.
        QueryExpr::Source(_) => {}

        // Filter — push label predicates to Agent.
        QueryExpr::Filter { pred, input } => {
            collect_label_filters_into(pred, &mut plan.agent.label_filters);
            walk(input, plan, budgets);
        }

        // Window — time window lives at Agent.
        QueryExpr::Window { duration, input, .. } => {
            plan.agent.window_secs = Some(duration.as_secs());
            walk(input, plan, budgets);
        }

        // SketchAgg — the key sketch assignment decision.
        QueryExpr::SketchAgg { op, input, .. } => {
            assign_sketch_agg(op, plan, budgets);
            walk(input, plan, budgets);
        }

        // WindowedAgg — bundles window + sketch agg intent.
        QueryExpr::WindowedAgg { agg, window, input, .. } => {
            if let crate::algebra::expr::WindowKind::Tumbling { size } = &window.kind {
                plan.agent.window_secs = Some(size.as_secs());
            }
            assign_sketch_agg(agg, plan, budgets);
            walk(input, plan, budgets);
        }

        // Partition — GROUP BY / `by (dims)` always assigned to Backend.
        QueryExpr::Partition { keys, input } => {
            for k in keys.keys() {
                if !plan.backend.group_by.contains(k) {
                    plan.backend.group_by.push(k.clone());
                }
            }
            walk(input, plan, budgets);
        }

        // Aggregate — SQL GROUP BY + agg functions.
        // Mergeable aggs (Sum/Count/Min/Max) → Backend.
        // Non-mergeable (Avg/StdDev/Variance) → Db.
        // Sketchable (Quantile/CountDistinct/HeavyHitters) → treated as SketchAgg.
        QueryExpr::Aggregate { keys, aggs, input, .. } => {
            for k in keys {
                if !plan.backend.group_by.contains(k) {
                    plan.backend.group_by.push(k.clone());
                }
            }
            for agg in aggs {
                assign_agg_func(&agg.func, plan, budgets);
            }
            walk(input, plan, budgets);
        }

        // Dedup — absorbed at Backend (HLL dedup elimination is upstream).
        QueryExpr::Dedup { input, .. } => {
            plan.backend.has_dedup = true;
            walk(input, plan, budgets);
        }

        // Merge — Backend merges N agent sketches.
        QueryExpr::Merge { inputs } => {
            plan.backend.has_merge = true;
            for i in inputs {
                walk(i, plan, budgets);
            }
        }

        // TopK — always at the Precompute Engine.
        QueryExpr::TopK { k, input, .. } => {
            plan.precompute.topk = Some(*k);
            plan.precompute.active = true;
            walk(input, plan, budgets);
        }

        // Sort + Limit — maps to topk semantics at Precompute.
        QueryExpr::Limit { n, input, .. } => {
            plan.precompute.topk = Some(*n);
            plan.precompute.active = true;
            walk(input, plan, budgets);
        }
        QueryExpr::Sort { input, .. } => {
            walk(input, plan, budgets);
        }

        // HistogramQuantile — precompute engine applies it over ingested histograms.
        QueryExpr::HistogramQuantile { input, .. } => {
            plan.precompute.active = true;
            walk(input, plan, budgets);
        }

        // PromQLSubquery — precompute engine evaluates the sub-query.
        QueryExpr::PromQLSubquery { input, .. } => {
            plan.precompute.active = true;
            walk(input, plan, budgets);
        }

        // BinaryOp between two instant vectors — precompute evaluates.
        QueryExpr::BinaryOp { lhs, rhs, .. } => {
            plan.precompute.active = true;
            walk(lhs, plan, budgets);
            walk(rhs, plan, budgets);
        }

        // JoinSketch — outer and inner both walked; join itself at Backend.
        QueryExpr::JoinSketch { outer, inner, .. } => {
            plan.backend.has_merge = true;
            walk(outer, plan, budgets);
            walk(inner, plan, budgets);
        }

        // Join — Backend.
        QueryExpr::Join { left, right, .. } => {
            plan.backend.has_merge = true;
            walk(left, plan, budgets);
            walk(right, plan, budgets);
        }

        // SetOp — treat as Backend merge.
        QueryExpr::SetOp { left, right, .. } => {
            plan.backend.has_merge = true;
            walk(left, plan, budgets);
            walk(right, plan, budgets);
        }

        // Transparent / passthrough nodes — recurse into child.
        QueryExpr::Project { input, .. }
        | QueryExpr::Subquery { expr: input, .. }
        | QueryExpr::WindowFunc { input, .. } => walk(input, plan, budgets),

        QueryExpr::LetBinding { expr, body, .. } => {
            walk(expr, plan, budgets);
            walk(body, plan, budgets);
        }

        // Ref — nothing to assign (resolved externally).
        QueryExpr::Ref(_) => {}
    }
}

// ── Agg assignment ────────────────────────────────────────────────────────────

fn assign_sketch_agg(op: &AggIntent, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    match op {
        // Exact ops: mergeability decides stage.
        AggIntent::Exact(ExactAgg::Sum | ExactAgg::Count | ExactAgg::Min | ExactAgg::Max) => {
            plan.backend.has_merge = true;
        }
        AggIntent::Exact(ExactAgg::Avg) => {
            plan.db.active = true;
        }
        // Sketch ops: resolve to physical, assign to Agent, defer if budget exceeded.
        sketch_op => {
            let physical = crate::algebra::physical::resolve(sketch_op);
            let stage = resolve_sketch_stage(physical.estimated_memory_bytes, budgets, &mut plan.deferral_log, sketch_op);
            match stage {
                SketchStage::Agent => {
                    plan.agent.sketch_type   = Some(physical.sketch_type);
                    plan.agent.sketch_params = physical.sketch_params;
                }
                SketchStage::Backend => {
                    plan.backend.has_merge = true;
                }
                SketchStage::Precompute => {
                    plan.precompute.active = true;
                }
            }
        }
    }
}

fn assign_agg_func(func: &AggFunc, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    match func {
        // Sketchable → synthesise the corresponding AggIntent and use existing logic.
        AggFunc::Quantile(phi) => {
            let op = AggIntent::default_quantile(vec![*phi]);
            assign_sketch_agg(&op, plan, budgets);
        }
        AggFunc::CountDistinct => {
            assign_sketch_agg(&AggIntent::default_cardinality(), plan, budgets);
        }
        AggFunc::HeavyHitters { .. } => {
            assign_sketch_agg(&AggIntent::default_frequency(), plan, budgets);
        }
        // Mergeable exact → Backend.
        AggFunc::Count | AggFunc::Sum | AggFunc::Min | AggFunc::Max
        | AggFunc::Rate | AggFunc::Increase | AggFunc::Delta => {
            plan.backend.has_merge = true;
        }
        // Non-mergeable → Db.
        AggFunc::Avg | AggFunc::StdDev { .. } | AggFunc::Variance { .. } => {
            plan.db.active = true;
        }
        AggFunc::Custom(_) => {
            // Unknown; conservatively route to Precompute.
            plan.precompute.active = true;
        }
    }
}

// ── Budget deferral ───────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
enum SketchStage { Agent, Backend, Precompute }

fn resolve_sketch_stage(
    est_mem: u64,
    budgets: &StageResourceBudgets,
    log: &mut Vec<String>,
    op: &AggIntent,
) -> SketchStage {
    if let Some(cap) = budgets.agent_memory_bytes {
        if est_mem > cap {
            log.push(format!(
                "deferred {op:?} Agent→Backend: est_mem={est_mem}B > agent_cap={cap}B"
            ));
            if let Some(be_cap) = budgets.backend_memory_bytes {
                if est_mem > be_cap {
                    log.push(format!(
                        "deferred {op:?} Backend→Precompute: est_mem={est_mem}B > backend_cap={be_cap}B"
                    ));
                    return SketchStage::Precompute;
                }
            }
            return SketchStage::Backend;
        }
    }
    SketchStage::Agent
}

// Sketch-type helpers delegated to algebra::directory.

// ── PromQL serialiser — recursive descent ─────────────────────────────────────

/// Mutable context threaded through the recursive descent.
/// Carries `group_by` and `window` that are "collected" from inner nodes
/// and applied at the enclosing aggregate.
#[derive(Default, Clone)]
struct PromQLCtx {
    /// GROUP BY / `by (…)` labels gathered from `Partition` nodes above.
    group_by: Vec<String>,
    /// Time window gathered from the innermost `Window` node.
    window: Option<Duration>,
}

/// Recursive PromQL serialisation of a [`QueryExpr`] node.
///
/// Returns the PromQL string fragment for this node.  Inner nodes (Source,
/// Filter) return their selector string; outer nodes (SketchAgg, TopK, etc.)
/// wrap it.
fn promql_from_qe(expr: &QueryExpr, ctx: &mut PromQLCtx) -> String {
    match expr {
        // ── Leaf ─────────────────────────────────────────────────────────────
        QueryExpr::Source(s) => s.name.clone(),
        QueryExpr::Ref(name) => name.clone(),

        // ── Filter — append label matchers to the selector ───────────────────
        QueryExpr::Filter { pred, input } => {
            let inner = promql_from_qe(input, ctx);
            let matchers = scalar_to_label_matchers(pred);
            if matchers.is_empty() {
                inner
            } else {
                format!("{}{{{}}}", inner, matchers.join(", "))
            }
        }

        // ── Window — store duration for use by enclosing aggregate ───────────
        QueryExpr::Window { duration, input, .. } => {
            if ctx.window.is_none() {
                ctx.window = Some(*duration);
            }
            promql_from_qe(input, ctx)
        }

        // ── Partition — store group_by keys for enclosing aggregate ──────────
        QueryExpr::Partition { keys, input } => {
            for k in keys.keys() {
                if !ctx.group_by.contains(k) {
                    ctx.group_by.push(k.clone());
                }
            }
            promql_from_qe(input, ctx)
        }

        // ── WindowedAgg — bundled window + sketch agg ────────────────────────
        QueryExpr::WindowedAgg { agg, window, input, .. } => {
            if ctx.window.is_none() {
                if let crate::algebra::expr::WindowKind::Tumbling { size } = &window.kind {
                    ctx.window = Some(*size);
                }
            }
            let selector = promql_from_qe(input, ctx);
            let window_s = window_str(ctx.window);
            let by       = by_clause(&ctx.group_by);
            sketch_op_to_promql(agg, &selector, &window_s, &by)
        }

        // ── SketchAgg — the main aggregation node ────────────────────────────
        QueryExpr::SketchAgg { op, input, .. } => {
            let selector = promql_from_qe(input, ctx);
            let window   = window_str(ctx.window);
            let by       = by_clause(&ctx.group_by);
            sketch_op_to_promql(op, &selector, &window, &by)
        }

        // ── Aggregate (SQL GROUP BY) ──────────────────────────────────────────
        QueryExpr::Aggregate { keys, aggs, input, .. } => {
            // Merge SQL GROUP BY keys into the context.
            for k in keys {
                if !ctx.group_by.contains(k) {
                    ctx.group_by.push(k.clone());
                }
            }
            let selector = promql_from_qe(input, ctx);
            let window   = window_str(ctx.window);
            let by       = by_clause(&ctx.group_by);
            // Use the first aggregate function to drive the PromQL template.
            if let Some(agg) = aggs.first() {
                agg_func_to_promql(&agg.func, &selector, &window, &by)
            } else {
                selector
            }
        }

        // ── TopK ─────────────────────────────────────────────────────────────
        QueryExpr::TopK { k, input, .. } => {
            let inner = promql_from_qe(input, ctx);
            format!("topk({k}, {inner})")
        }

        // ── Sort + Limit — map to topk ───────────────────────────────────────
        QueryExpr::Limit { n, input, .. } => {
            let inner = promql_from_qe(input, ctx);
            format!("topk({n}, {inner})")
        }
        QueryExpr::Sort { input, .. } => promql_from_qe(input, ctx),

        // ── histogram_quantile(φ, rate(selector[w])) ─────────────────────────
        QueryExpr::HistogramQuantile { phi, input } => {
            let selector = promql_from_qe(input, ctx);
            let window   = window_str(ctx.window);
            format!("histogram_quantile({phi}, rate({selector}{window}))")
        }

        // ── PromQL subquery expr[range:step] ─────────────────────────────────
        QueryExpr::PromQLSubquery { range, resolution, input } => {
            let inner    = promql_from_qe(input, ctx);
            let step_str = resolution
                .map(|r| format!(":{}", format_duration(r)))
                .unwrap_or_default();
            format!("{}[{}{}]", inner, format_duration(*range), step_str)
        }

        // ── Vector binary op  (lhs op rhs) ───────────────────────────────────
        QueryExpr::BinaryOp { op, lhs, rhs, vector_match } => {
            let lhs_str = promql_from_qe(lhs, ctx);
            let rhs_str = promql_from_qe(rhs, &mut PromQLCtx::default());
            let op_str  = binop_to_promql(op);
            let match_str = vector_match
                .as_ref()
                .map(|m| {
                    use crate::algebra::expr::{GroupSide, VectorMatchKind};
                    let kw = match m.kind {
                        VectorMatchKind::On       => "on",
                        VectorMatchKind::Ignoring => "ignoring",
                    };
                    let labels = m.labels.join(", ");
                    let group = m.grouping.as_ref().map(|g| {
                        let side = match g.side {
                            GroupSide::Left  => "group_left",
                            GroupSide::Right => "group_right",
                        };
                        if g.labels.is_empty() {
                            format!(" {side}")
                        } else {
                            format!(" {side}({})", g.labels.join(", "))
                        }
                    }).unwrap_or_default();
                    format!(" {kw} ({labels}){group}")
                })
                .unwrap_or_default();
            format!("({lhs_str} {op_str}{match_str} {rhs_str})")
        }

        // ── Merge — serialise first branch (all branches same shape) ─────────
        QueryExpr::Merge { inputs } => {
            inputs.first()
                .map(|first| promql_from_qe(first, ctx))
                .unwrap_or_default()
        }

        // ── Passthrough nodes ─────────────────────────────────────────────────
        QueryExpr::Dedup { input, .. }
        | QueryExpr::Project { input, .. }
        | QueryExpr::WindowFunc { input, .. } => promql_from_qe(input, ctx),

        QueryExpr::Subquery { expr, .. } => promql_from_qe(expr, ctx),

        QueryExpr::LetBinding { body, .. } => promql_from_qe(body, ctx),

        // ── Join / SetOp — serialise the outer / left branch ─────────────────
        QueryExpr::JoinSketch { outer, .. } => promql_from_qe(outer, ctx),
        QueryExpr::Join       { left,  .. } => promql_from_qe(left,  ctx),
        QueryExpr::SetOp      { left,  .. } => promql_from_qe(left,  ctx),
    }
}

// ── PromQL fragment helpers ───────────────────────────────────────────────────

fn window_str(w: Option<Duration>) -> String {
    w.map(|d| format!("[{}]", format_duration(d))).unwrap_or_default()
}

fn by_clause(keys: &[String]) -> String {
    if keys.is_empty() {
        String::new()
    } else {
        format!(" by ({})", keys.join(", "))
    }
}

fn sketch_op_to_promql(op: &AggIntent, selector: &str, window: &str, by: &str) -> String {
    match op {
        AggIntent::Quantile { quantiles, .. } => {
            let phi = quantiles.first().copied().unwrap_or(0.99);
            format!("quantile_over_time({phi}, {selector}{window}){by}")
        }
        AggIntent::Cardinality { .. } => {
            format!("count_over_time({selector}{window}){by}")
        }
        AggIntent::Frequency { .. } => {
            format!("count_over_time({selector}{window}){by}")
        }
        AggIntent::Extrema { min, max } => match (min, max) {
            (true, false) => format!("min_over_time({selector}{window}){by}"),
            (false, true) => format!("max_over_time({selector}{window}){by}"),
            _             => format!("quantile_over_time(0.5, {selector}{window}){by}"),
        },
        AggIntent::Exact(ExactAgg::Count) => format!("count_over_time({selector}{window}){by}"),
        AggIntent::Exact(ExactAgg::Sum)   => format!("sum_over_time({selector}{window}){by}"),
        AggIntent::Exact(ExactAgg::Avg)   => format!("avg_over_time({selector}{window}){by}"),
        AggIntent::Exact(ExactAgg::Min)   => format!("min_over_time({selector}{window}){by}"),
        AggIntent::Exact(ExactAgg::Max)   => format!("max_over_time({selector}{window}){by}"),
        AggIntent::PerPartition { inner, .. } => sketch_op_to_promql(inner, selector, window, by),
    }
}

fn agg_func_to_promql(func: &AggFunc, selector: &str, window: &str, by: &str) -> String {
    match func {
        AggFunc::Quantile(phi)   => format!("quantile_over_time({phi}, {selector}{window}){by}"),
        AggFunc::CountDistinct   => format!("count_over_time({selector}{window}){by}"),
        AggFunc::HeavyHitters{k} => format!("topk({k}, count_over_time({selector}{window}){by})"),
        AggFunc::Count           => format!("count_over_time({selector}{window}){by}"),
        AggFunc::Sum             => format!("sum_over_time({selector}{window}){by}"),
        AggFunc::Avg             => format!("avg_over_time({selector}{window}){by}"),
        AggFunc::Min             => format!("min_over_time({selector}{window}){by}"),
        AggFunc::Max             => format!("max_over_time({selector}{window}){by}"),
        AggFunc::StdDev { .. }  => format!("stddev_over_time({selector}{window}){by}"),
        AggFunc::Variance { .. } => format!("stdvar_over_time({selector}{window}){by}"),
        AggFunc::Rate            => format!("rate({selector}{window}){by}"),
        AggFunc::Increase        => format!("increase({selector}{window}){by}"),
        AggFunc::Delta           => format!("delta({selector}{window}){by}"),
        AggFunc::Custom(name)    => format!("{name}({selector}{window}){by}"),
    }
}

fn binop_to_promql(op: &BinaryOpKind) -> &'static str {
    match op {
        BinaryOpKind::Add    => "+",
        BinaryOpKind::Sub    => "-",
        BinaryOpKind::Mul    => "*",
        BinaryOpKind::Div    => "/",
        BinaryOpKind::Mod    => "%",
        BinaryOpKind::Pow    => "^",
        BinaryOpKind::Eq     => "==",
        BinaryOpKind::Ne     => "!=",
        BinaryOpKind::Lt     => "<",
        BinaryOpKind::Le     => "<=",
        BinaryOpKind::Gt     => ">",
        BinaryOpKind::Ge     => ">=",
        BinaryOpKind::And    => "and",
        BinaryOpKind::Or     => "or",
        BinaryOpKind::Unless => "unless",
        BinaryOpKind::Atan2  => "atan2",
        _                    => "and", // bitwise/string ops not in PromQL
    }
}

// ── Label matcher extraction from ScalarExpr ─────────────────────────────────

/// Extract PromQL-compatible label matchers from a `ScalarExpr` AND-tree.
///
/// Only equality / inequality / regex comparisons between a `Column` and a
/// string `Literal` are extractable as label matchers.  Everything else is
/// silently ignored (it won't become a label filter in the PromQL output).
fn scalar_to_label_matchers(pred: &ScalarExpr) -> Vec<String> {
    let mut out = Vec::new();
    collect_label_matchers(pred, &mut out);
    out
}

fn collect_label_matchers(pred: &ScalarExpr, out: &mut Vec<String>) {
    match pred {
        // AND-tree: recurse into both sides.
        ScalarExpr::BinaryOp { op: BinaryOpKind::And, lhs, rhs } => {
            collect_label_matchers(lhs, out);
            collect_label_matchers(rhs, out);
        }
        // col = "val"
        ScalarExpr::BinaryOp { op: BinaryOpKind::Eq, lhs, rhs } => {
            if let (ScalarExpr::Column(col), ScalarExpr::Literal(LiteralValue::Str(v)))
                = (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{}=\"{}\"", col, v));
            }
        }
        // col != "val"
        ScalarExpr::BinaryOp { op: BinaryOpKind::Ne, lhs, rhs } => {
            if let (ScalarExpr::Column(col), ScalarExpr::Literal(LiteralValue::Str(v)))
                = (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{}!=\"{}\"", col, v));
            }
        }
        // col =~ "regex"
        ScalarExpr::BinaryOp { op: BinaryOpKind::Regex, lhs, rhs } => {
            if let (ScalarExpr::Column(col), ScalarExpr::Literal(LiteralValue::Str(v)))
                = (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{}=~\"{}\"", col, v));
            }
        }
        // col !~ "regex"
        ScalarExpr::BinaryOp { op: BinaryOpKind::NotRegex, lhs, rhs } => {
            if let (ScalarExpr::Column(col), ScalarExpr::Literal(LiteralValue::Str(v)))
                = (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{}!~\"{}\"", col, v));
            }
        }
        _ => {}
    }
}

// ── Helper: push label filters from a ScalarExpr into a Vec<String> ──────────

fn collect_label_filters_into(pred: &ScalarExpr, out: &mut Vec<String>) {
    let matchers = scalar_to_label_matchers(pred);
    for m in matchers {
        // Store as "col=val" (without PromQL quotes) for the agent YAML.
        // The YAML generator already re-quotes as needed.
        if !out.contains(&m) {
            out.push(m);
        }
    }
}

// ── Phase E: typed L5 call path (opt-in, additive) ────────────────────────────

/// Env-var that opts the planner into the typed L5 stage_split path
/// (`crate::stage_split::StageAllocator` + `ThreeStageEmitter`).
/// Additive — when unset, the existing untyped `split_expr_by_stage`
/// flow runs unchanged. Mirror of `ENV_USE_TYPED_SKETCH_ALGEBRA` from
/// Phase C.
///
/// Set `USE_TYPED_STAGE_SPLIT=1` to opt in.
#[allow(dead_code)]
pub const ENV_USE_TYPED_STAGE_SPLIT: &str = "USE_TYPED_STAGE_SPLIT";

/// Whether the typed L5 stage_split path is enabled for this process.
/// Reads the env var once per call (cheap; called per `plan()`
/// invocation at most). Phase E is additive — both code paths produce
/// per-stage descriptions, but the typed path's structural output is
/// `crate::stage_split::StageConfig` (sketched against design.md §6),
/// while the legacy path is the existing `StagedPlan` shape.
///
/// Phase B (MVP v6) wires `main::handle_plan` to consult this gate.
pub fn typed_stage_split_enabled() -> bool {
    matches!(
        std::env::var(ENV_USE_TYPED_STAGE_SPLIT).as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

/// Run the typed L5 path on a Phase-C-bound `SketchExpr` DAG. Returns
/// the per-stage [`crate::stage_split::StageConfig`] map for the DC
/// lifecycle topology.
///
/// Returns `None` when the typed path errors out (unsupported topology
/// shape, unresolved Ref, empty backend) — the caller should then fall
/// back to the legacy `split_expr_by_stage` output.
///
/// Phase B (MVP v6) wires this into `main::handle_plan` behind the
/// `USE_TYPED_STAGE_SPLIT` env-var gate. Each per-stage config the
/// returned map carries is materialised into wire bytes by the
/// emitters in [`crate::config::stage_config`] —
/// [`crate::config::stage_config::emit_edge_yaml`] for `Edge`,
/// [`crate::config::stage_config::emit_gateway_yaml`] for `Gateway`,
/// [`crate::config::stage_config::emit_backend_config_json`] for
/// `Backend`. Phase C plumbs deployment-aware endpoint resolution.
pub fn split_typed_three_stage(
    expr: &crate::sketch_algebra::SketchExpr,
) -> Option<std::collections::HashMap<crate::stage_split::StageId, crate::stage_split::StageConfig>>
{
    use crate::stage_split::{
        Emitter, StageAllocator, ThreeStageEmitter, Topology,
    };
    let dag = StageAllocator.allocate(expr, Topology::ThreeStage).ok()?;
    ThreeStageEmitter.emit_per_stage(&dag).ok()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::expr::{AggItem, AggIntent, BinaryOpKind, LiteralValue, QueryExpr, ScalarExpr};
    use crate::algebra::expr::{ColumnRef, PartitionKeys, SourceSpec};

    fn source(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    fn no_budget() -> StageResourceBudgets { StageResourceBudgets::default() }

    fn eq_filter(col: &str, val: &str) -> QueryExpr {
        QueryExpr::Filter {
            pred: ScalarExpr::BinaryOp {
                op:  BinaryOpKind::Eq,
                lhs: Box::new(ScalarExpr::Column(col.into())),
                rhs: Box::new(ScalarExpr::Literal(LiteralValue::Str(val.into()))),
            },
            input: Box::new(source("latency")),
        }
    }

    // ── Node-to-stage assignment ──────────────────────────────────────────────

    #[test]
    fn ddsketch_agg_goes_to_agent() {
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide: None,
            input: Box::new(QueryExpr::SketchAgg {
                op:    AggIntent::default_quantile(vec![0.99]),
                col:   ColumnRef::SampleValue,
                input: Box::new(source("latency")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::DDSketch));
        assert_eq!(plan.agent.window_secs, Some(300));
        assert!(!plan.precompute.active);
        assert!(!plan.db.active);
    }

    #[test]
    fn hll_stays_at_agent_by_default() {
        let expr = QueryExpr::SketchAgg {
            op:    AggIntent::default_cardinality(),
            col:   ColumnRef::SampleValue,
            input: Box::new(source("events")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::HLL));
    }

    #[test]
    fn partition_group_by_goes_to_backend() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["host".into(), "region".into()]),
            input: Box::new(QueryExpr::SketchAgg {
                op:    AggIntent::default_quantile(vec![0.99]),
                col:   ColumnRef::SampleValue,
                input: Box::new(source("latency")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.group_by.contains(&"host".to_string()));
        assert!(plan.backend.group_by.contains(&"region".to_string()));
    }

    #[test]
    fn aggregate_without_group_by_avg_goes_to_db() {
        let expr = QueryExpr::Aggregate {
            keys:   vec![],
            aggs:   vec![AggItem {
                alias:    "avg_val".into(),
                func:     AggFunc::Avg,
                col:      ColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input:  Box::new(source("price")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.db.active);
    }

    #[test]
    fn aggregate_sum_goes_to_backend() {
        let expr = QueryExpr::Aggregate {
            keys:   vec!["symbol".into()],
            aggs:   vec![AggItem {
                alias:    "total".into(),
                func:     AggFunc::Sum,
                col:      ColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input:  Box::new(source("trades")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.has_merge);
        assert!(plan.backend.group_by.contains(&"symbol".to_string()));
        assert!(!plan.db.active);
    }

    #[test]
    fn topk_goes_to_precompute() {
        let expr = QueryExpr::TopK {
            k:     10,
            by:    vec!["symbol".into()],
            input: Box::new(QueryExpr::SketchAgg {
                op:    AggIntent::default_frequency(),
                col:   ColumnRef::SampleValue,
                input: Box::new(source("price")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
        assert_eq!(plan.precompute.topk, Some(10));
    }

    #[test]
    fn histogram_quantile_activates_precompute() {
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.99,
            input: Box::new(QueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(source("http_request_duration_seconds_bucket")),
            }),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
        assert!(!plan.precompute.query_expr.is_empty());
        assert!(plan.precompute.query_expr.contains("histogram_quantile(0.99"));
    }

    #[test]
    fn binary_op_activates_precompute() {
        let lhs = source("metric_a");
        let rhs = source("metric_b");
        let expr = QueryExpr::BinaryOp {
            op:           BinaryOpKind::Div,
            lhs:          Box::new(lhs),
            rhs:          Box::new(rhs),
            vector_match: None,
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
    }

    // ── Budget-driven deferral ────────────────────────────────────────────────

    #[test]
    fn ddsketch_deferred_to_backend_when_agent_budget_exceeded() {
        let tiny_budget = StageResourceBudgets {
            agent_memory_bytes: Some(1), // 1 byte — DDSketch (4 KiB) won't fit
            ..Default::default()
        };
        let expr = QueryExpr::SketchAgg {
            op:    AggIntent::default_quantile(vec![0.99]),
            col:   ColumnRef::SampleValue,
            input: Box::new(source("latency")),
        };
        let plan = split_expr_by_stage(&expr, &tiny_budget);
        assert_eq!(plan.agent.sketch_type, None); // not at agent
        assert!(plan.backend.has_merge);          // deferred to backend
        assert!(!plan.deferral_log.is_empty());
    }

    #[test]
    fn ddsketch_deferred_to_precompute_when_both_budgets_exceeded() {
        let tiny_budget = StageResourceBudgets {
            agent_memory_bytes:   Some(1),
            backend_memory_bytes: Some(1),
            ..Default::default()
        };
        let expr = QueryExpr::SketchAgg {
            op:    AggIntent::default_quantile(vec![0.99]),
            col:   ColumnRef::SampleValue,
            input: Box::new(source("latency")),
        };
        let plan = split_expr_by_stage(&expr, &tiny_budget);
        assert!(plan.precompute.active);
        assert_eq!(plan.deferral_log.len(), 2); // two deferral steps logged
    }

    // ── expr_to_promql ────────────────────────────────────────────────────────

    #[test]
    fn promql_ddsketch_with_filter_and_window() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["symbol".into()]),
            input: Box::new(QueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(QueryExpr::SketchAgg {
                    op:    AggIntent::default_quantile(vec![0.99]),
                    col:   ColumnRef::SampleValue,
                    input: Box::new(QueryExpr::Filter {
                        pred: ScalarExpr::BinaryOp {
                            op:  BinaryOpKind::Eq,
                            lhs: Box::new(ScalarExpr::Column("sectype".into())),
                            rhs: Box::new(ScalarExpr::Literal(LiteralValue::Str("E".into()))),
                        },
                        input: Box::new(source("price")),
                    }),
                }),
            }),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("quantile_over_time(0.99"), "expected quantile_over_time: {ql}");
        assert!(ql.contains("sectype=\"E\""),            "expected label filter: {ql}");
        assert!(ql.contains("[5m]"),                     "expected window: {ql}");
        assert!(ql.contains("by (symbol)"),              "expected group_by: {ql}");
    }

    #[test]
    fn promql_topk_wraps_inner() {
        let expr = QueryExpr::TopK {
            k:     10,
            by:    vec![],
            input: Box::new(QueryExpr::SketchAgg {
                op:    AggIntent::default_frequency(),
                col:   ColumnRef::SampleValue,
                input: Box::new(source("events")),
            }),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.starts_with("topk(10,"), "expected topk prefix: {ql}");
    }

    #[test]
    fn promql_histogram_quantile() {
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.95,
            input: Box::new(QueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(source("http_request_duration_seconds_bucket")),
            }),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("histogram_quantile(0.95"), "got: {ql}");
        assert!(ql.contains("rate("),                   "got: {ql}");
        assert!(ql.contains("[5m]"),                    "got: {ql}");
    }

    #[test]
    fn promql_binary_op_renders_operator() {
        let expr = QueryExpr::BinaryOp {
            op:           BinaryOpKind::Div,
            lhs:          Box::new(source("http_errors")),
            rhs:          Box::new(source("http_requests")),
            vector_match: None,
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains('/'), "expected / operator: {ql}");
        assert!(ql.contains("http_errors"),   "got: {ql}");
        assert!(ql.contains("http_requests"), "got: {ql}");
    }

    #[test]
    fn promql_subquery_renders_range() {
        let expr = QueryExpr::PromQLSubquery {
            range:      Duration::from_secs(3600),
            resolution: Some(Duration::from_secs(60)),
            input:      Box::new(source("metric")),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains("[1h:1m]") || ql.contains("[3600s:60s]"), "got: {ql}");
    }

    #[test]
    fn label_matchers_and_tree() {
        let pred = ScalarExpr::BinaryOp {
            op: BinaryOpKind::And,
            lhs: Box::new(ScalarExpr::BinaryOp {
                op:  BinaryOpKind::Eq,
                lhs: Box::new(ScalarExpr::Column("job".into())),
                rhs: Box::new(ScalarExpr::Literal(LiteralValue::Str("api".into()))),
            }),
            rhs: Box::new(ScalarExpr::BinaryOp {
                op:  BinaryOpKind::Ne,
                lhs: Box::new(ScalarExpr::Column("env".into())),
                rhs: Box::new(ScalarExpr::Literal(LiteralValue::Str("dev".into()))),
            }),
        };
        let matchers = scalar_to_label_matchers(&pred);
        assert!(matchers.contains(&"job=\"api\"".to_string()));
        assert!(matchers.contains(&"env!=\"dev\"".to_string()));
    }

    // ── Phase E: typed L5 opt-in path ──────────────────────────────────────

    #[test]
    fn typed_three_stage_path_returns_three_configs() {
        use crate::intent_algebra::schema::{Column, DataType};
        use crate::intent_algebra::{
            LabelFilter, QueryExpr as L3QE, Schema, Source as L3Source, WindowKind,
        };
        use crate::sketch_algebra::params::{KllParams, SketchKind, SketchParams as L4Params};
        use crate::sketch_algebra::sketch_expr::{EstimateOp, SketchExpr};
        use crate::stage_split::StageId;
        let scan = L3QE::Scan {
            source: L3Source::TimeSeries {
                metric: "http_request_duration_seconds".into(),
            },
            label_filters: vec![LabelFilter {
                label: "service".into(),
                equals: "api".into(),
            }],
            schema: Schema::with_time_index(
                vec![
                    Column {
                        name: "ts".into(),
                        dtype: DataType::Timestamp,
                        nullable: false,
                    },
                    Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                    },
                ],
                0,
                vec![vec![0]],
            ),
        };
        let windowed = L3QE::Window {
            kind: WindowKind::Sliding,
            size: std::time::Duration::from_secs(300),
            slide: None,
            child: Box::new(scan),
        };
        let l4 = SketchExpr::estimate_over_agg(
            EstimateOp::Quantile { q: 0.99 },
            SketchKind::Kll,
            L4Params::Kll(KllParams { k: 200 }),
            windowed,
        );
        let configs = super::split_typed_three_stage(&l4).expect("typed path produces output");
        assert!(configs.contains_key(&StageId::Edge));
        assert!(configs.contains_key(&StageId::Backend));
    }
}
