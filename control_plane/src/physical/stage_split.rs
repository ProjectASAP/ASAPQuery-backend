//! SP-9: AST-aware hierarchical stage assignment.
//!
//! Splits an optimised [`QueryExpr`] tree across pipeline stages, emitting a
//! [`StagedPlan`] that carries per-stage sub-plans for:
//!
//! | Stage | Nodes |
//! |---|---|
//! | Agent OTel Collector | `Scan`, `Filter`, `Window`, single-intent sketch `Aggregate` |
//! | Backend OTel Collector | `Partition`, `Merge`, `Distinct`, mergeable-exact `Aggregate` |
//! | ASAPQuery Precompute Engine | `TopK`-intent `Aggregate`, `Subquery`, deferred sketch ops |
//! | DB-side query | non-mergeable-exact `Aggregate` (`Avg`) |
//!
//! # Exact-op deferral
//!
//! Mergeability drives exact-intent placement:
//! - `Sum`, `Count`, `Min`, `Max` — mergeable (`agg(A∪B) = merge(agg(A), agg(B))`) →
//!   **Backend** (the backend collector can combine partial results from N agents).
//! - `Avg` — **not** mergeable → **DB-side** query.
//!
//! # Budget-driven deferral chain
//!
//! When a sketch intent's estimated memory cost exceeds the stage cap in
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
//! consumed by the ASAPQuery Precompute Engine's query engine.  It uses a
//! proper recursive descent so it handles `Subquery` and vector `BinaryOp`
//! nodes natively.
//!
//! Step γ7: this module consumes the canonical `query_expr::QueryExpr`. The
//! legacy `SketchAgg` / `WindowedAgg` / `TopK` variants fold into canonical
//! `Aggregate` / `Window { Aggregate }`, so the single `Aggregate` arm
//! dispatches on intent shape and the `Window` arm is a plain passthrough
//! (the inner `Aggregate` carries the sketch decision).

use std::time::Duration;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::legacy_expr::{agg_is_exact, agg_is_mergeable};
use crate::intent_algebra::query_expr::{
    BinaryOpKind, ColumnRef, GroupSide, LiteralValue, Predicate, QueryExpr, Source,
    VectorMatchKind,
};
use crate::pipeline::format_duration;
use crate::physical::sketch_catalog;
use crate::types::{
    AgentSubPlan, BackendSubPlan, DbSubPlan, PrecomputeSubPlan, SketchParams, SketchType,
    StagedPlan, StageResourceBudgets,
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Split a [`QueryExpr`] tree across pipeline stages, respecting per-stage
/// resource budgets and the intent mergeability rules described above.
///
/// The returned [`StagedPlan`] is attached to
/// [`crate::types::CollectionPlan::staged_plan`] by the caller
/// (`handle_plan` in `main.rs`).
pub fn split_expr_by_stage(expr: &QueryExpr, budgets: &StageResourceBudgets) -> StagedPlan {
    let mut plan = StagedPlan::default();
    walk(expr, &mut plan, budgets);

    // Build the precompute query_expr from the full tree when the precompute
    // stage is active (TopK, Subquery, or deferred ops).
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
/// Uses recursive descent, so it handles `Subquery` and vector `BinaryOp`
/// nodes.
pub fn expr_to_promql(expr: &QueryExpr) -> String {
    let mut ctx = PromQLCtx::default();
    promql_from_qe(expr, &mut ctx)
}

// ── Tree walker ───────────────────────────────────────────────────────────────

fn walk(expr: &QueryExpr, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    match expr {
        // Scan / Ref — leaves, nothing to assign.
        QueryExpr::Scan { .. } | QueryExpr::Ref { .. } => {}

        // Filter — push label predicates to Agent.
        QueryExpr::Filter { pred, child } => {
            collect_label_filters_into(pred, &mut plan.agent.label_filters);
            walk(child, plan, budgets);
        }

        // Window — time window lives at Agent. A `Window` over a
        // single-intent `Aggregate` is the canonical fold of the legacy
        // `WindowedAgg`; the inner `Aggregate` arm carries the sketch
        // decision, so this arm just records the window seconds.
        QueryExpr::Window { size, child, .. } => {
            plan.agent.window_secs = Some(size.as_secs());
            walk(child, plan, budgets);
        }

        // Partition — GROUP BY / `by (dims)` always assigned to Backend.
        QueryExpr::Partition { keys, child } => {
            for k in keys.keys() {
                if !plan.backend.group_by.contains(k) {
                    plan.backend.group_by.push(k.clone());
                }
            }
            walk(child, plan, budgets);
        }

        // Aggregate — the canonical fold of legacy SketchAgg / WindowedAgg-
        // inner / TopK / Aggregate. Each intent is assigned in turn:
        // TopK → Precompute; everything else → sketch / exact placement.
        QueryExpr::Aggregate { aggs, child, .. } => {
            for intent in aggs {
                if let AggIntent::TopK { k, .. } = intent {
                    plan.precompute.topk = Some(*k as u64);
                    plan.precompute.active = true;
                } else {
                    assign_sketch_agg(intent, plan, budgets);
                }
            }
            walk(child, plan, budgets);
        }

        // Distinct — absorbed at Backend (HLL dedup elimination is upstream).
        QueryExpr::Distinct { child, .. } => {
            plan.backend.has_dedup = true;
            walk(child, plan, budgets);
        }

        // Merge — Backend merges N agent sketches.
        QueryExpr::Merge { children } => {
            plan.backend.has_merge = true;
            for c in children {
                walk(c, plan, budgets);
            }
        }

        // Limit — maps to topk semantics at Precompute.
        QueryExpr::Limit { n, child, .. } => {
            plan.precompute.topk = Some(*n as u64);
            plan.precompute.active = true;
            walk(child, plan, budgets);
        }
        QueryExpr::Sort { child, .. } => {
            walk(child, plan, budgets);
        }

        // Subquery — precompute engine evaluates the sub-query.
        QueryExpr::Subquery { child, .. } => {
            plan.precompute.active = true;
            walk(child, plan, budgets);
        }

        // BinaryOp between two instant vectors — precompute evaluates.
        QueryExpr::BinaryOp { lhs, rhs, .. } => {
            plan.precompute.active = true;
            walk(lhs, plan, budgets);
            walk(rhs, plan, budgets);
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
        QueryExpr::Project { child, .. } => walk(child, plan, budgets),

        QueryExpr::LetBinding { expr, child, .. } => {
            walk(expr, plan, budgets);
            walk(child, plan, budgets);
        }
    }
}

// ── Agg assignment ────────────────────────────────────────────────────────────

fn assign_sketch_agg(op: &AggIntent, plan: &mut StagedPlan, budgets: &StageResourceBudgets) {
    // Exact ops: mergeability decides stage. Canonical mergeable exact
    // intents (Sum / Count / Min / Max) → Backend; non-mergeable (Avg) → Db.
    if agg_is_exact(op) {
        if agg_is_mergeable(op) {
            plan.backend.has_merge = true;
        } else {
            plan.db.active = true;
        }
        return;
    }

    // Sketch ops: resolve to physical, assign to Agent, defer if budget exceeded.
    let physical = crate::physical::planner::resolve(op);
    let stage = resolve_sketch_stage(
        physical.estimated_memory_bytes,
        budgets,
        &mut plan.deferral_log,
        op,
    );
    match stage {
        SketchStage::Agent => {
            plan.agent.sketch_type = Some(physical.sketch_type);
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

// ── Budget deferral ───────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
enum SketchStage {
    Agent,
    Backend,
    Precompute,
}

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
/// Returns the PromQL string fragment for this node.  Inner nodes (Scan,
/// Filter) return their selector string; outer nodes (Aggregate, etc.)
/// wrap it.
fn promql_from_qe(expr: &QueryExpr, ctx: &mut PromQLCtx) -> String {
    match expr {
        // ── Leaf ─────────────────────────────────────────────────────────────
        QueryExpr::Scan { source, .. } => match source {
            Source::TimeSeries { metric } => metric.clone(),
            Source::Table { table_ref } => table_ref.clone(),
        },
        QueryExpr::Ref { name } => name.as_str().to_string(),

        // ── Filter — append label matchers to the selector ───────────────────
        QueryExpr::Filter { pred, child } => {
            let inner = promql_from_qe(child, ctx);
            let matchers = scalar_to_label_matchers(pred);
            if matchers.is_empty() {
                inner
            } else {
                format!("{}{{{}}}", inner, matchers.join(", "))
            }
        }

        // ── Window — store duration for use by enclosing aggregate ───────────
        QueryExpr::Window { size, child, .. } => {
            if ctx.window.is_none() {
                ctx.window = Some(*size);
            }
            promql_from_qe(child, ctx)
        }

        // ── Partition — store group_by keys for enclosing aggregate ──────────
        QueryExpr::Partition { keys, child } => {
            for k in keys.keys() {
                if !ctx.group_by.contains(k) {
                    ctx.group_by.push(k.clone());
                }
            }
            promql_from_qe(child, ctx)
        }

        // ── Aggregate (the canonical fold of SketchAgg / WindowedAgg-inner /
        // TopK / general Aggregate) ──────────────────────────────────────────
        QueryExpr::Aggregate { aggs, child, .. } => {
            let selector = promql_from_qe(child, ctx);
            let window = window_str(ctx.window);
            let by = by_clause(&ctx.group_by);
            match aggs.first() {
                Some(AggIntent::TopK { k, .. }) => format!("topk({k}, {selector})"),
                Some(intent) => sketch_op_to_promql(intent, &selector, &window, &by),
                None => selector,
            }
        }

        // ── Limit — maps to topk ─────────────────────────────────────────────
        QueryExpr::Limit { n, child, .. } => {
            let inner = promql_from_qe(child, ctx);
            format!("topk({n}, {inner})")
        }
        QueryExpr::Sort { child, .. } => promql_from_qe(child, ctx),

        // ── Subquery  expr[range:step] ───────────────────────────────────────
        QueryExpr::Subquery {
            range,
            resolution,
            child,
        } => {
            let inner = promql_from_qe(child, ctx);
            let step_str = resolution
                .map(|r| format!(":{}", format_duration(r)))
                .unwrap_or_default();
            format!("{}[{}{}]", inner, format_duration(*range), step_str)
        }

        // ── Vector binary op  (lhs op rhs) ───────────────────────────────────
        QueryExpr::BinaryOp {
            op,
            lhs,
            rhs,
            vector_match,
        } => {
            let lhs_str = promql_from_qe(lhs, ctx);
            let rhs_str = promql_from_qe(rhs, &mut PromQLCtx::default());
            let op_str = binop_to_promql(op);
            let match_str = vector_match
                .as_ref()
                .map(|m| {
                    let kw = match m.kind {
                        VectorMatchKind::On => "on",
                        VectorMatchKind::Ignoring => "ignoring",
                    };
                    let labels = m.labels.join(", ");
                    let group = m
                        .grouping
                        .as_ref()
                        .map(|g| {
                            let side = match g.side {
                                GroupSide::Left => "group_left",
                                GroupSide::Right => "group_right",
                            };
                            if g.labels.is_empty() {
                                format!(" {side}")
                            } else {
                                format!(" {side}({})", g.labels.join(", "))
                            }
                        })
                        .unwrap_or_default();
                    format!(" {kw} ({labels}){group}")
                })
                .unwrap_or_default();
            format!("({lhs_str} {op_str}{match_str} {rhs_str})")
        }

        // ── Merge — serialise first branch (all branches same shape) ─────────
        QueryExpr::Merge { children } => children
            .first()
            .map(|first| promql_from_qe(first, ctx))
            .unwrap_or_default(),

        // ── Passthrough nodes ─────────────────────────────────────────────────
        QueryExpr::Distinct { child, .. } | QueryExpr::Project { child, .. } => {
            promql_from_qe(child, ctx)
        }

        QueryExpr::LetBinding { child, .. } => promql_from_qe(child, ctx),

        // ── Join / SetOp — serialise the outer / left branch ─────────────────
        QueryExpr::Join { left, .. } => promql_from_qe(left, ctx),
        QueryExpr::SetOp { left, .. } => promql_from_qe(left, ctx),
    }
}

// ── PromQL fragment helpers ───────────────────────────────────────────────────

fn window_str(w: Option<Duration>) -> String {
    w.map(|d| format!("[{}]", format_duration(d)))
        .unwrap_or_default()
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
        AggIntent::Quantile { q, .. } => {
            format!("quantile_over_time({q}, {selector}{window}){by}")
        }
        AggIntent::Cardinality { .. } => {
            format!("count_over_time({selector}{window}){by}")
        }
        AggIntent::Frequency { .. } => {
            format!("count_over_time({selector}{window}){by}")
        }
        AggIntent::Count { .. } => format!("count_over_time({selector}{window}){by}"),
        AggIntent::Sum => format!("sum_over_time({selector}{window}){by}"),
        AggIntent::Avg => format!("avg_over_time({selector}{window}){by}"),
        AggIntent::Min => format!("min_over_time({selector}{window}){by}"),
        AggIntent::Max => format!("max_over_time({selector}{window}){by}"),
        AggIntent::Rate { .. } => format!("rate({selector}{window}){by}"),
        AggIntent::Increase { .. } => format!("increase({selector}{window}){by}"),
        // TopK + archive-only intents: Step γ routes these via canonical
        // templates; today they reuse `count_over_time` because the legacy
        // `Exact(_)` fall-through did effectively the same for any
        // non-mergeable case. Documented as a Step γ TODO.
        _ => format!("count_over_time({selector}{window}){by}"),
    }
}

fn binop_to_promql(op: &BinaryOpKind) -> &'static str {
    match op {
        BinaryOpKind::Add => "+",
        BinaryOpKind::Sub => "-",
        BinaryOpKind::Mul => "*",
        BinaryOpKind::Div => "/",
        BinaryOpKind::Mod => "%",
        BinaryOpKind::Pow => "^",
        BinaryOpKind::Eq => "==",
        BinaryOpKind::Ne => "!=",
        BinaryOpKind::Lt => "<",
        BinaryOpKind::Le => "<=",
        BinaryOpKind::Gt => ">",
        BinaryOpKind::Ge => ">=",
        BinaryOpKind::And => "and",
        BinaryOpKind::Or => "or",
        BinaryOpKind::Unless => "unless",
        BinaryOpKind::Atan2 => "atan2",
        _ => "and", // bitwise/string ops not in PromQL
    }
}

// ── Label matcher extraction from Predicate ──────────────────────────────────

/// Extract PromQL-compatible label matchers from a `Predicate` AND-tree.
///
/// Only equality / inequality / regex comparisons between a `Column` and a
/// string `Literal` are extractable as label matchers.  Everything else is
/// silently ignored (it won't become a label filter in the PromQL output).
fn scalar_to_label_matchers(pred: &Predicate) -> Vec<String> {
    let mut out = Vec::new();
    collect_label_matchers(pred, &mut out);
    out
}

fn collect_label_matchers(pred: &Predicate, out: &mut Vec<String>) {
    match pred {
        // AND-tree: recurse into both sides.
        Predicate::BinaryOp {
            op: BinaryOpKind::And,
            lhs,
            rhs,
        } => {
            collect_label_matchers(lhs, out);
            collect_label_matchers(rhs, out);
        }
        // col = "val"
        Predicate::BinaryOp {
            op: BinaryOpKind::Eq,
            lhs,
            rhs,
        } => {
            if let (Predicate::Column(ColumnRef::Named(col)), Predicate::Literal(LiteralValue::Str(v))) =
                (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{col}=\"{v}\""));
            }
        }
        // col != "val"
        Predicate::BinaryOp {
            op: BinaryOpKind::Ne,
            lhs,
            rhs,
        } => {
            if let (Predicate::Column(ColumnRef::Named(col)), Predicate::Literal(LiteralValue::Str(v))) =
                (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{col}!=\"{v}\""));
            }
        }
        // col =~ "regex"
        Predicate::BinaryOp {
            op: BinaryOpKind::Regex,
            lhs,
            rhs,
        } => {
            if let (Predicate::Column(ColumnRef::Named(col)), Predicate::Literal(LiteralValue::Str(v))) =
                (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{col}=~\"{v}\""));
            }
        }
        // col !~ "regex"
        Predicate::BinaryOp {
            op: BinaryOpKind::NotRegex,
            lhs,
            rhs,
        } => {
            if let (Predicate::Column(ColumnRef::Named(col)), Predicate::Literal(LiteralValue::Str(v))) =
                (lhs.as_ref(), rhs.as_ref())
            {
                out.push(format!("{col}!~\"{v}\""));
            }
        }
        _ => {}
    }
}

// ── Helper: push label filters from a Predicate into a Vec<String> ───────────

fn collect_label_filters_into(pred: &Predicate, out: &mut Vec<String>) {
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
/// (`crate::physical::colored_dag::StageAllocator` + `ThreeStageEmitter`).
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
/// `crate::physical::colored_dag::StageConfig` (sketched against design.md §6),
/// while the legacy path is the existing `StagedPlan` shape.
///
/// Phase B (MVP v6) wires `main::handle_plan` to consult this gate.
pub fn typed_stage_split_enabled() -> bool {
    matches!(
        std::env::var(ENV_USE_TYPED_STAGE_SPLIT).as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

/// Run the typed L5 path on a Phase-C-bound `PhysicalExpr` DAG. Returns
/// the per-stage [`crate::physical::colored_dag::StageConfig`] map for the DC
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
    expr: &crate::sketch_algebra::PhysicalExpr,
) -> Option<
    std::collections::HashMap<
        crate::physical::colored_dag::StageId,
        crate::physical::colored_dag::StageConfig,
    >,
> {
    use crate::physical::colored_dag::{Emitter, StageAllocator, ThreeStageEmitter, Topology};
    let dag = StageAllocator.allocate(expr, Topology::ThreeStage).ok()?;
    ThreeStageEmitter.emit_per_stage(&dag).ok()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::{
        default_cardinality, default_frequency, default_quantile,
    };
    use crate::intent_algebra::{PartitionKeys, Schema, Source, WindowKind};
    use crate::types_v2::AccuracyTarget;

    fn no_budget() -> StageResourceBudgets {
        StageResourceBudgets::default()
    }

    /// Canonical `Scan` leaf.
    fn scan(name: &str) -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: name.into(),
            },
            label_filters: vec![],
            schema: Schema::default(),
        }
    }

    /// Single-intent, global, no-HAVING `Aggregate` over a `Scan` — the
    /// canonical fold of the legacy `SketchAgg`.
    fn sketch_agg(intent: AggIntent, metric: &str) -> QueryExpr {
        QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![intent],
            having: None,
            child: Box::new(scan(metric)),
        }
    }

    /// `Window { Aggregate }` — the canonical fold of the legacy
    /// `WindowedAgg`.
    fn windowed_agg(intent: AggIntent, size_secs: u64, metric: &str) -> QueryExpr {
        QueryExpr::Window {
            kind: WindowKind::Tumbling,
            size: Duration::from_secs(size_secs),
            slide: None,
            child: Box::new(sketch_agg(intent, metric)),
        }
    }

    fn eq_pred(col: &str, val: &str) -> Predicate {
        Predicate::BinaryOp {
            op: BinaryOpKind::Eq,
            lhs: Box::new(Predicate::Column(ColumnRef::Named(col.into()))),
            rhs: Box::new(Predicate::Literal(LiteralValue::Str(val.into()))),
        }
    }

    // ── Node-to-stage assignment ──────────────────────────────────────────────

    #[test]
    fn ddsketch_agg_goes_to_agent() {
        let expr = windowed_agg(default_quantile(0.99), 300, "latency");
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::DDSketch));
        assert_eq!(plan.agent.window_secs, Some(300));
        assert!(!plan.precompute.active);
        assert!(!plan.db.active);
    }

    #[test]
    fn hll_stays_at_agent_by_default() {
        let expr = sketch_agg(default_cardinality(), "events");
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert_eq!(plan.agent.sketch_type, Some(SketchType::HLL));
    }

    #[test]
    fn partition_group_by_goes_to_backend() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["host".into(), "region".into()]),
            child: Box::new(sketch_agg(default_quantile(0.99), "latency")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.group_by.contains(&"host".to_string()));
        assert!(plan.backend.group_by.contains(&"region".to_string()));
    }

    #[test]
    fn aggregate_avg_goes_to_db() {
        // Single-intent Avg → non-mergeable exact → Db.
        let expr = sketch_agg(AggIntent::Avg, "price");
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.db.active);
    }

    #[test]
    fn aggregate_sum_goes_to_backend() {
        // Single-intent Sum → mergeable exact → Backend.
        let expr = sketch_agg(AggIntent::Sum, "trades");
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.backend.has_merge);
        assert!(!plan.db.active);
    }

    #[test]
    fn topk_goes_to_precompute() {
        let expr = QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            }],
            having: None,
            child: Box::new(sketch_agg(default_frequency(), "price")),
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
        assert_eq!(plan.precompute.topk, Some(10));
    }

    #[test]
    fn binary_op_activates_precompute() {
        let expr = QueryExpr::BinaryOp {
            op: BinaryOpKind::Div,
            lhs: Box::new(scan("metric_a")),
            rhs: Box::new(scan("metric_b")),
            vector_match: None,
        };
        let plan = split_expr_by_stage(&expr, &no_budget());
        assert!(plan.precompute.active);
    }

    // ── Budget-driven deferral ────────────────────────────────────────────────

    #[test]
    fn ddsketch_deferred_to_backend_when_agent_budget_exceeded() {
        let tiny_budget = StageResourceBudgets {
            agent_memory_bytes: Some(1), // 1 byte — DDSketch won't fit
            ..Default::default()
        };
        let expr = sketch_agg(default_quantile(0.99), "latency");
        let plan = split_expr_by_stage(&expr, &tiny_budget);
        assert_eq!(plan.agent.sketch_type, None); // not at agent
        assert!(plan.backend.has_merge); // deferred to backend
        assert!(!plan.deferral_log.is_empty());
    }

    #[test]
    fn ddsketch_deferred_to_precompute_when_both_budgets_exceeded() {
        let tiny_budget = StageResourceBudgets {
            agent_memory_bytes: Some(1),
            backend_memory_bytes: Some(1),
            ..Default::default()
        };
        let expr = sketch_agg(default_quantile(0.99), "latency");
        let plan = split_expr_by_stage(&expr, &tiny_budget);
        assert!(plan.precompute.active);
        assert_eq!(plan.deferral_log.len(), 2); // two deferral steps logged
    }

    // ── expr_to_promql ────────────────────────────────────────────────────────

    #[test]
    fn promql_ddsketch_with_filter_and_window() {
        let expr = QueryExpr::Partition {
            keys: PartitionKeys::By(vec!["symbol".into()]),
            child: Box::new(QueryExpr::Window {
                kind: WindowKind::Tumbling,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(QueryExpr::Aggregate {
                    by: vec![],
                    aggs: vec![default_quantile(0.99)],
                    having: None,
                    child: Box::new(QueryExpr::Filter {
                        pred: eq_pred("sectype", "E"),
                        child: Box::new(scan("price")),
                    }),
                }),
            }),
        };
        let ql = expr_to_promql(&expr);
        assert!(
            ql.contains("quantile_over_time(0.99"),
            "expected quantile_over_time: {ql}"
        );
        assert!(ql.contains("sectype=\"E\""), "expected label filter: {ql}");
        assert!(ql.contains("[5m]"), "expected window: {ql}");
        assert!(ql.contains("by (symbol)"), "expected group_by: {ql}");
    }

    #[test]
    fn promql_topk_wraps_inner() {
        let expr = QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            }],
            having: None,
            child: Box::new(sketch_agg(default_frequency(), "events")),
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.starts_with("topk(10,"), "expected topk prefix: {ql}");
    }

    #[test]
    fn promql_binary_op_renders_operator() {
        let expr = QueryExpr::BinaryOp {
            op: BinaryOpKind::Div,
            lhs: Box::new(scan("http_errors")),
            rhs: Box::new(scan("http_requests")),
            vector_match: None,
        };
        let ql = expr_to_promql(&expr);
        assert!(ql.contains('/'), "expected / operator: {ql}");
        assert!(ql.contains("http_errors"), "got: {ql}");
        assert!(ql.contains("http_requests"), "got: {ql}");
    }

    #[test]
    fn promql_subquery_renders_range() {
        let expr = QueryExpr::Subquery {
            range: Duration::from_secs(3600),
            resolution: Some(Duration::from_secs(60)),
            child: Box::new(scan("metric")),
        };
        let ql = expr_to_promql(&expr);
        assert!(
            ql.contains("[1h:1m]") || ql.contains("[3600s:60s]"),
            "got: {ql}"
        );
    }

    #[test]
    fn label_matchers_and_tree() {
        let pred = Predicate::BinaryOp {
            op: BinaryOpKind::And,
            lhs: Box::new(eq_pred("job", "api")),
            rhs: Box::new(Predicate::BinaryOp {
                op: BinaryOpKind::Ne,
                lhs: Box::new(Predicate::Column(ColumnRef::Named("env".into()))),
                rhs: Box::new(Predicate::Literal(LiteralValue::Str("dev".into()))),
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
        use crate::physical::colored_dag::StageId;
        use crate::sketch_algebra::params::{KllParams, SketchKind, SketchParams as L4Params};
        use crate::sketch_algebra::physical_expr::{EstimateOp, PhysicalExpr};
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
        let l4 = PhysicalExpr::estimate_over_agg(
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
