//! Layers 1→2 lowering: PromQL string → QueryExpr (relational plan).
//!
//! - **Layer 1**: the `promql-parser` crate parses the PromQL string into a
//!   language-specific AST (`promql_parser::parser::Expr`).
//! - **Layer 2**: the walk functions (`walk_qe`, `walk_call_qe`, `walk_aggregate_qe`)
//!   interpret PromQL semantics (range vectors, aggregation operators, label matchers)
//!   and emit relational operators (`Aggregate { AggFunc }` + `Window`).
//!
//! The output is a Layer 2 `QueryExpr` tree — the same relational operators that
//! the SQL parser emits.  A shared lowering pass (`algebra::lower`) converts
//! `Aggregate { AggFunc }` → `SketchAgg { AggIntent }` for both languages.
//!
//! # PromQL → AggFunc mapping (summary)
//!
//! | Expression | AggFunc |
//! |---|---|
//! | `quantile_over_time(φ, m[w])` | Quantile(φ) |
//! | `histogram_quantile(φ, rate(m[w]))` | Quantile(φ)  (parser-level substitution; see step γ5) |
//! | `avg_over_time(m[w])` | Avg |
//! | `min_over_time(m[w])` | Min |
//! | `max_over_time(m[w])` | Max |
//! | `stddev/stdvar_over_time(m[w])` | StdDev / Variance |
//! | `count_over_time(m[w])` | Count / CountDistinct (context) |
//! | `sum_over_time(m[w])` | Sum |
//! | `last_over_time / delta / deriv / predict_linear` | Delta / Sum (exact) |
//! | `changes / resets` | Count |
//! | `rate / irate / increase` | Rate / Increase |
//! | `topk(k, …)` outer | TopK (structural — not AggFunc) |
//! | `count(…over_time… by (d))` outer | CountDistinct |
//! | `m{filters}` bare | Sum (exact) |
//! | `m_a op m_b` binary | (BinaryOp — not an Aggregate) |

use std::time::Duration;

use anyhow::anyhow;
use promql_parser::parser::{self, AggregateExpr, Call, Expr, LabelModifier, VectorSelector};

use crate::intent_algebra::relational::{FilterOp, FilterVal, Predicate};

// ── Walk context ──────────────────────────────────────────────────────────────

/// PromQL `by(labels)` / `without(labels)` aggregation modifier, accumulated
/// as we descend the AST. Control_plane-only walking state — `asap_l2`'s
/// `relational::QueryExpr::Aggregate` has no separate `Partition` node to
/// mirror this against (its `keys`/`without` fields live directly on
/// `Aggregate`, see `intent_algebra::relational`'s module doc), so this
/// folds straight into the nearest `Aggregate`/`Window` via
/// [`fold_group_mod`] instead of wrapping a dedicated node.
#[derive(Clone)]
enum GroupMod {
    By(Vec<String>),
    Without(Vec<String>),
}

impl GroupMod {
    fn keys(&self) -> &[String] {
        match self {
            GroupMod::By(k) | GroupMod::Without(k) => k,
        }
    }
    fn is_empty(&self) -> bool {
        self.keys().is_empty()
    }
}

/// Context accumulated as we descend the AST.
#[derive(Default, Clone)]
struct WalkCtx {
    /// GROUP BY / `without` clause from an outer Aggregate node.
    partition: Option<GroupMod>,
    /// Top-K k from an outer `topk` / `bottomk` operator.
    topk: Option<u64>,
    /// Whether the outer context is a `count()` aggregate (→ CountDistinct).
    outer_count: bool,
}

// ── Helpers: MatrixSelector extraction ───────────────────────────────────────

/// Extract `(metric_name, filters, window)` from a MatrixSelector argument at
/// position `arg_idx` of a Call.
fn extract_matrix_arg(
    call: &Call,
    arg_idx: usize,
) -> anyhow::Result<(String, Vec<Predicate>, Duration)> {
    let arg = call
        .args
        .args
        .get(arg_idx)
        .map(|b| b.as_ref())
        .ok_or_else(|| anyhow!("missing arg {} in call to {}", arg_idx, call.func.name))?;
    extract_inner_matrix(arg)
}

/// Walk into an expression until we find a MatrixSelector, then extract its info.
fn extract_inner_matrix(expr: &Expr) -> anyhow::Result<(String, Vec<Predicate>, Duration)> {
    match expr {
        Expr::MatrixSelector(ms) => {
            let (name, filters) = extract_vs_info(&ms.vs);
            Ok((name, filters, ms.range))
        }
        Expr::Paren(p) => extract_inner_matrix(p.expr.as_ref()),
        Expr::Call(c) => {
            // rate/irate wraps a MatrixSelector.
            extract_inner_matrix(c.args.args[0].as_ref())
        }
        other => Err(anyhow!(
            "expected MatrixSelector, got {:?}",
            std::mem::discriminant(other)
        )),
    }
}

// ── Helpers: VectorSelector info ─────────────────────────────────────────────

fn extract_vs_info(vs: &VectorSelector) -> (String, Vec<Predicate>) {
    // Metric name: prefer the explicit name field, fall back to __name__ matcher.
    let name = vs.name.clone().unwrap_or_else(|| {
        vs.matchers
            .matchers
            .iter()
            .find(|m| m.name == "__name__")
            .map(|m| m.value.clone())
            .unwrap_or_default()
    });

    let filters = vs
        .matchers
        .matchers
        .iter()
        .filter(|m| m.name != "__name__")
        .filter_map(matcher_to_predicate)
        .collect();

    (name, filters)
}

fn matcher_to_predicate(m: &promql_parser::label::Matcher) -> Option<Predicate> {
    use promql_parser::label::MatchOp;
    let (op, val) = match &m.op {
        MatchOp::Equal => (FilterOp::Eq, FilterVal::Str(m.value.clone())),
        MatchOp::NotEqual => (FilterOp::Ne, FilterVal::Str(m.value.clone())),
        MatchOp::Re(re) => (
            FilterOp::Regex(re.to_string()),
            FilterVal::Str(m.value.clone()),
        ),
        MatchOp::NotRe(re) => (
            FilterOp::NotRegex(re.to_string()),
            FilterVal::Str(m.value.clone()),
        ),
    };
    Some(Predicate {
        col: m.name.clone(),
        op,
        val,
    })
}

// ── Helpers: number extraction ────────────────────────────────────────────────

fn extract_call_num_arg(call: &Call, idx: usize) -> anyhow::Result<f64> {
    match call.args.args.get(idx).map(|b| b.as_ref()) {
        Some(Expr::NumberLiteral(n)) => Ok(n.val),
        Some(other) => Err(anyhow!(
            "expected number at arg {} of {}, got {:?}",
            idx,
            call.func.name,
            std::mem::discriminant(other)
        )),
        None => Err(anyhow!("missing arg {} in {}", idx, call.func.name)),
    }
}

fn extract_number_param(param: &Option<Box<Expr>>) -> anyhow::Result<f64> {
    match param {
        Some(e) => match e.as_ref() {
            Expr::NumberLiteral(n) => Ok(n.val),
            other => Err(anyhow!(
                "expected number param, got {:?}",
                std::mem::discriminant(other)
            )),
        },
        None => Err(anyhow!("missing required numeric parameter")),
    }
}

// ── Helpers: GroupMod from LabelModifier ──────────────────────────────────────

fn modifier_to_partition(modifier: &LabelModifier) -> GroupMod {
    match modifier {
        LabelModifier::Include(labels) => GroupMod::By(labels.labels.clone()),
        LabelModifier::Exclude(labels) => GroupMod::Without(labels.labels.clone()),
    }
}

// ── Direct QueryExpr emission ─────────────────────────────────────────────────
//
// `parse_promql_expr` walks the same PromQL AST but emits [`QueryExpr`] nodes
// natively, preserving semantic nodes for the algebra optimizer:
//
// | PromQL pattern           | QueryExpr node                        |
// |--------------------------|---------------------------------------|
// | `histogram_quantile(φ…)` | Aggregate { Quantile(φ) } (step γ5)   |
// | `m[5m:1m]` subquery      | PromQLSubquery { 5m, Some(1m) }      |
// | `a op b` binary          | BinaryOp { VectorMatch }             |

use crate::intent_algebra::relational::{
    AggFunc, AggItem, BinaryOpKind, ColumnRef as QeColumnRef, GroupSide, L2SortKey, QueryExpr,
    SourceSpec as QeSourceSpec, VectorGrouping, VectorMatch, VectorMatchKind,
};
use crate::intent_algebra::{
    is_frequency_heavy_hitter, ArithOp, CompareOp, L2Expr, RankingMeasure,
};
use promql_parser::parser::{token::TokenType, BinaryExpr, VectorMatchCardinality};

/// Parse a PromQL expression string directly into an optimised [`QueryExpr`].
///
/// This preserves `PromQLSubquery` and `BinaryOp` nodes natively;
/// `histogram_quantile(φ, …)` is substituted into a plain
/// `Aggregate { Quantile(φ) }` per Step γ5 of the relational migration.
pub fn parse_promql_expr(query: &str) -> anyhow::Result<QueryExpr> {
    let expr = parser::parse(query).map_err(|e| anyhow!("PromQL parse error: {e}"))?;
    walk_qe(&expr, WalkCtx::default())
}

fn walk_qe(expr: &Expr, ctx: WalkCtx) -> anyhow::Result<QueryExpr> {
    match expr {
        Expr::Aggregate(agg) => walk_aggregate_qe(agg, ctx),
        Expr::Call(call) => walk_call_qe(call, ctx),

        // Binary op: map to QueryExpr::BinaryOp with VectorMatch.
        Expr::Binary(bin) => walk_binary_qe(bin),

        Expr::Paren(p) => walk_qe(p.expr.as_ref(), ctx),

        // Subquery `expr[range:resolution]` → PromQLSubquery.
        Expr::Subquery(sq) => {
            let inner = walk_qe(sq.expr.as_ref(), ctx.clone())?;
            Ok(QueryExpr::PromQLSubquery {
                range: sq.range,
                resolution: sq.step,
                input: Box::new(inner),
            })
        }

        // Bare vector selector → Source + Filter, with `Aggregate(Sum)`
        // wrapping ONLY when the outer context is value-aggregating
        // (sum / avg / min / max / etc.). PromQL's `count(metric)`
        // operates on the result-set's LABEL-SETS — the inner
        // selector is just "the things to count," not a value to sum.
        // PromQL's `topk(k, metric)` operates on the SERIES — the
        // inner selector is the population to rank, not a value to
        // sum. Synthesizing `Aggregate(Sum)` underneath either outer
        // would collect a redundant `ExactAgg(Sum)` candidate
        // alongside the intended `CardinalityApprox` /
        // `FrequencyTopk(*WithHeap)` one; the engine's
        // "all candidates must succeed" semantic then surfaces a
        // `CapabilityMiss` when no Sum policy is registered (e.g. an
        // HLL-only or CMS-with-heap-only deploy).
        Expr::VectorSelector(vs) => {
            let (name, filters) = extract_vs_info(vs);
            let source = QueryExpr::Source(QeSourceSpec::new(name));
            let filtered = apply_qe_filters(source, filters);
            if ctx.outer_count || ctx.topk.is_some() {
                Ok(filtered)
            } else {
                Ok(QueryExpr::Aggregate {
                    keys: vec![],
                    without: false,
                    aggs: vec![AggItem {
                        alias: Some("value".into()),
                        func: AggFunc::Sum,
                        col: QeColumnRef::SampleValue,
                    }],
                    having: None,
                    input: Box::new(filtered),
                })
            }
        }

        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => Err(anyhow!(
            "unexpected literal at top level of PromQL expression"
        )),

        #[allow(unreachable_patterns)]
        _ => Err(anyhow!("unsupported PromQL expression type")),
    }
}

/// Whether `expr` (a `topk`/`bottomk` argument) is `count_over_time(...)`,
/// possibly parenthesized — the one PromQL shape ranked by
/// `RankingMeasure::Frequency`, the only measure with a realised
/// heavy-hitter sketch today (`agg_intent::is_frequency_heavy_hitter`).
/// A bare `count(...)` doesn't qualify: it's a *cross-series* reduction
/// (one value, not per-series), so ranking by it isn't a per-series
/// heavy-hitter shape in the first place.
fn is_count_over_time(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(p) => is_count_over_time(p.expr.as_ref()),
        Expr::Call(c) => c.func.name == "count_over_time",
        _ => false,
    }
}

fn walk_aggregate_qe(agg: &AggregateExpr, ctx: WalkCtx) -> anyhow::Result<QueryExpr> {
    let partition = agg.modifier.as_ref().map(modifier_to_partition);
    let op_name = format!("{}", agg.op);

    match op_name.as_str() {
        "topk" | "bottomk" => {
            let k = extract_number_param(&agg.param)? as u64;
            let descending = op_name == "topk";
            // A ranking is the heavy-hitter `TopK` intent only when it
            // takes the *top* k (`descending` — `bottomk` never
            // qualifies) *and* ranks by a measure with a realised
            // heavy-hitter sketch — today, unweighted frequency
            // (`count_over_time(...)`) only. Every other measure
            // (avg/quantile/rate/a bare selector/...) is
            // `RankingMeasure::NonAdditive` and falls through to a
            // generic `Sort + Limit` order-by-value below — matching
            // ASAPController's `frontend-promql` design (issue #38: "the
            // descending-plus-measure rule is shared with the L3
            // canonicalize promotion so the two cannot drift"). Before
            // this, `topk(k, <anything>)` unconditionally forced a
            // `Count`-shaped inner regardless of what was actually being
            // ranked — silently wrong for `topk(k, avg_over_time(...))`
            // and friends.
            let measure = if is_count_over_time(agg.expr.as_ref()) {
                RankingMeasure::Frequency
            } else {
                RankingMeasure::NonAdditive
            };
            if is_frequency_heavy_hitter(descending, measure) {
                let inner_ctx = WalkCtx {
                    partition: partition.clone(),
                    topk: Some(k),
                    outer_count: false,
                };
                let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
                // Don't fold the group keys into a separate wrapper here —
                // the inner Aggregate already has them (or will, once its
                // own construction site folds `partition` in).
                return Ok(QueryExpr::TopK {
                    k,
                    by: partition
                        .as_ref()
                        .map(|p| p.keys().iter().cloned().map(QeColumnRef::Named).collect())
                        .unwrap_or_default(),
                    input: Box::new(inner),
                });
            }
            // Generic order-by-value + limit: `bottomk`, and any `topk`
            // ranking by a non-count measure. `ctx.topk` stays `None` so
            // `build_qe_aggregate` doesn't force a `Count`-shaped inner.
            let inner_ctx = WalkCtx {
                partition: partition.clone(),
                topk: None,
                outer_count: false,
            };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let sorted = QueryExpr::Sort {
                keys: vec![L2SortKey {
                    expr: L2Expr::Column(QeColumnRef::SampleValue),
                    ascending: !descending,
                    nulls_first: false,
                }],
                partition_by: partition
                    .as_ref()
                    .map(|p| p.keys().iter().cloned().map(QeColumnRef::Named).collect())
                    .unwrap_or_default(),
                input: Box::new(inner),
            };
            let result = QueryExpr::Limit {
                n: k,
                offset: 0,
                input: Box::new(sorted),
            };
            Ok(result)
        }
        "count" => {
            let inner_ctx = WalkCtx {
                partition: partition.clone(),
                topk: None,
                outer_count: true,
            };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys: vec![],
                without: false,
                aggs: vec![AggItem {
                    alias: Some("count".into()),
                    func: AggFunc::CountDistinct,
                    col: QeColumnRef::SampleValue,
                }],
                having: None,
                input: Box::new(inner),
            };
            Ok(fold_group_mod(result, partition.as_ref()))
        }
        "sum" | "avg" | "min" | "max" | "group" => {
            let inner_ctx = WalkCtx {
                partition: partition.clone(),
                topk: ctx.topk,
                outer_count: false,
            };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            Ok(fold_group_mod(inner, partition.as_ref()))
        }
        "stddev" => {
            let inner_ctx = WalkCtx {
                partition: partition.clone(),
                topk: None,
                outer_count: false,
            };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys: vec![],
                without: false,
                aggs: vec![AggItem {
                    alias: Some("stddev".into()),
                    func: AggFunc::StdDev { population: false },
                    col: QeColumnRef::SampleValue,
                }],
                having: None,
                input: Box::new(inner),
            };
            Ok(fold_group_mod(result, partition.as_ref()))
        }
        "stdvar" => {
            let inner_ctx = WalkCtx {
                partition: partition.clone(),
                topk: None,
                outer_count: false,
            };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys: vec![],
                without: false,
                aggs: vec![AggItem {
                    alias: Some("stdvar".into()),
                    func: AggFunc::Variance { population: false },
                    col: QeColumnRef::SampleValue,
                }],
                having: None,
                input: Box::new(inner),
            };
            Ok(fold_group_mod(result, partition.as_ref()))
        }
        "quantile" => {
            let phi = extract_number_param(&agg.param)?;
            let inner_ctx = WalkCtx {
                partition: partition.clone(),
                topk: None,
                outer_count: false,
            };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys: vec![],
                without: false,
                aggs: vec![AggItem {
                    alias: Some("quantile".into()),
                    func: AggFunc::Quantile(phi),
                    col: QeColumnRef::SampleValue,
                }],
                having: None,
                input: Box::new(inner),
            };
            Ok(fold_group_mod(result, partition.as_ref()))
        }
        other => Err(anyhow!("unsupported PromQL aggregate operator: {other}")),
    }
}

fn walk_call_qe(call: &Call, ctx: WalkCtx) -> anyhow::Result<QueryExpr> {
    let name = call.func.name;
    match name {
        // histogram_quantile(φ, bucket_metric) → plain Aggregate { Quantile(φ) }.
        //
        // Per Step γ5 of the relational migration: at the PromQL parser level
        // we substitute `histogram_quantile(φ, bucket_metric)` with the same
        // shape that `quantile_over_time(φ, m[w])` produces — an `Aggregate`
        // carrying a single `AggFunc::Quantile(φ)`. Downstream code (the
        // L1→L3 lowerer, the optimizer, the physical planner) then sees a
        // plain Quantile and routes via the existing `AggIntent::Quantile`
        // path. Bucket-aware physical reduction is a physical-planner
        // concern, not an IR variant. The legacy `QueryExpr::HistogramQuantile`
        // variant has been retired.
        //
        // The inner `rate(...)` is the buckets argument; we use a fresh
        // `WalkCtx::default()` because an outer `topk` context would otherwise
        // rewrite the Quantile(φ) into a Count-frequency aggregate, which
        // would be semantically wrong for the histogram-quantile reduction.
        "histogram_quantile" => {
            let phi = extract_call_num_arg(call, 0)?;
            let rate_expr = call.args.args[1].as_ref();
            let (source, filters, window) = extract_inner_matrix(rate_expr)?;
            Ok(build_qe_aggregate(
                source,
                filters,
                window,
                AggFunc::Quantile(phi),
                WalkCtx::default(),
            ))
        }
        // All other function calls: map to AggFunc (Layer 2).
        "quantile_over_time" => {
            let phi = extract_call_num_arg(call, 0)?;
            let (source, filters, window) = extract_matrix_arg(call, 1)?;
            let func = AggFunc::Quantile(phi);
            Ok(build_qe_aggregate(source, filters, window, func, ctx))
        }
        // `predict_linear(v[w], t)` — `t` (seconds into the future) is a
        // scalar 2nd argument, so it doesn't fit `walk_call_to_op`'s
        // `(call, ctx, window)` shape; special-cased here like
        // `quantile_over_time`'s φ argument above.
        "predict_linear" => {
            let seconds = extract_call_num_arg(call, 1)?;
            let (source, filters, window) = extract_matrix_arg(call, 0)?;
            let func = AggFunc::PredictLinear { seconds };
            Ok(build_qe_aggregate(source, filters, window, func, ctx))
        }
        _ => {
            let (source, filters, window) = if call.func.name == "rate"
                || call.func.name == "irate"
                || call.func.name == "increase"
            {
                let arg = call
                    .args
                    .args
                    .first()
                    .map(|b| b.as_ref())
                    .ok_or_else(|| anyhow!("rate/irate/increase requires a matrix arg"))?;
                extract_inner_matrix(arg)?
            } else {
                extract_matrix_arg(call, 0)?
            };
            let func = walk_call_to_op(call, &ctx, window)?;
            Ok(build_qe_aggregate(source, filters, window, func, ctx))
        }
    }
}

fn walk_binary_qe(bin: &BinaryExpr) -> anyhow::Result<QueryExpr> {
    let lhs = walk_qe(bin.lhs.as_ref(), WalkCtx::default())?;
    let rhs = walk_qe(bin.rhs.as_ref(), WalkCtx::default())?;

    let op = promql_token_to_binop(bin.op);

    let vector_match = bin.modifier.as_ref().map(|m| {
        let (kind, labels) = match &m.matching {
            Some(LabelModifier::Include(ls)) => (VectorMatchKind::On, ls.labels.clone()),
            Some(LabelModifier::Exclude(ls)) => (VectorMatchKind::Ignoring, ls.labels.clone()),
            None => (VectorMatchKind::On, vec![]),
        };
        let grouping = match &m.card {
            VectorMatchCardinality::ManyToOne(ls) => Some(VectorGrouping {
                side: GroupSide::Left,
                labels: ls.labels.clone(),
            }),
            VectorMatchCardinality::OneToMany(ls) => Some(VectorGrouping {
                side: GroupSide::Right,
                labels: ls.labels.clone(),
            }),
            _ => None,
        };
        VectorMatch {
            kind,
            labels,
            grouping,
        }
    });

    Ok(QueryExpr::BinaryOp {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        vector_match,
    })
}

fn promql_token_to_binop(tok: TokenType) -> BinaryOpKind {
    use promql_parser::parser::token;
    // token::T_* are u8 constants; TokenType wraps them as TokenType(u8).
    let id = tok.id();
    match id {
        token::T_ADD => BinaryOpKind::Arith(ArithOp::Add),
        token::T_SUB => BinaryOpKind::Arith(ArithOp::Sub),
        token::T_MUL => BinaryOpKind::Arith(ArithOp::Mul),
        token::T_DIV => BinaryOpKind::Arith(ArithOp::Div),
        token::T_MOD => BinaryOpKind::Arith(ArithOp::Mod),
        token::T_POW => BinaryOpKind::Pow,
        token::T_EQLC => BinaryOpKind::Compare(CompareOp::Eq),
        token::T_NEQ => BinaryOpKind::Compare(CompareOp::Ne),
        token::T_LSS => BinaryOpKind::Compare(CompareOp::Lt),
        token::T_LTE => BinaryOpKind::Compare(CompareOp::Le),
        token::T_GTR => BinaryOpKind::Compare(CompareOp::Gt),
        token::T_GTE => BinaryOpKind::Compare(CompareOp::Ge),
        token::T_LAND => BinaryOpKind::And,
        token::T_LOR => BinaryOpKind::Or,
        token::T_LUNLESS => BinaryOpKind::Unless,
        token::T_ATAN2 => BinaryOpKind::Atan2,
        _ => BinaryOpKind::Arith(ArithOp::Add), // unknown — default to add
    }
}

/// Map a PromQL function call to an [`AggFunc`] (Layer 2 relational operator).
/// `window` is the range-vector's duration — only `Rate`/`Increase` carry it
/// on the `AggFunc` itself (`asap_l2`'s design: "no separate Window node").
/// Every other function still relies on the caller wrapping its `Aggregate`
/// in an `L2::Window`.
///
/// `predict_linear` is handled separately in `walk_call_qe` (its 2nd,
/// scalar argument doesn't fit this function's `(call, ctx, window)`
/// shape, matching how `quantile_over_time`/`histogram_quantile`'s φ
/// argument is already special-cased there).
fn walk_call_to_op(call: &Call, ctx: &WalkCtx, window: Duration) -> anyhow::Result<AggFunc> {
    let name = call.func.name;
    match name {
        "quantile_over_time" => {
            let phi = extract_call_num_arg(call, 0)?;
            Ok(AggFunc::Quantile(phi))
        }
        "avg_over_time" => Ok(AggFunc::Avg),
        "min_over_time" => Ok(AggFunc::Min),
        "max_over_time" => Ok(AggFunc::Max),
        "stddev_over_time" => Ok(AggFunc::StdDev { population: false }),
        "stdvar_over_time" => Ok(AggFunc::Variance { population: false }),
        "count_over_time" => {
            // Two cases, in priority order:
            //  * Inside `count by (...) (count_over_time(...))` →
            //    `CountDistinct` (HLL distinct counting; the outer
            //    count of inner counts is cardinality).
            //  * Otherwise → plain `Count`, still windowed here — the
            //    `lower.rs::agg_func_to_intents` grouped-or-windowed
            //    substitution recognizes the windowed shape and routes
            //    to CMS / CountSketch (per-series sample-count
            //    estimation), matching the pre-`asap_l2` behavior this
            //    used to reach via a dedicated `AggFunc::Frequency`
            //    variant that no longer exists. `topk(N, count_over_time
            //    (...))`'s heavy-hitter fusion is handled entirely in
            //    `walk_aggregate_qe`'s `topk`/`bottomk` arm now (it no
            //    longer forces `Count` here via `ctx.topk`).
            Ok(if ctx.outer_count {
                AggFunc::CountDistinct
            } else {
                AggFunc::Count
            })
        }
        "sum_over_time" => Ok(AggFunc::Sum),
        "last_over_time" => Ok(AggFunc::LastOverTime),
        "present_over_time" => Ok(AggFunc::PresentOverTime),
        "absent_over_time" => Ok(AggFunc::AbsentOverTime),
        // Each of these used to collapse onto `Delta` (delta/idelta/deriv)
        // or `Count` (changes/resets) — a crude placeholder bucketing from
        // before `asap_l2` gave every one of them its own dedicated
        // `AggFunc`/`AggIntent`. All are archive-only (no ASAP-tier
        // Bind* rule exists for any of them, same as before this fix) —
        // the correctness gain is `capability_for` now correctly
        // returning `None` (route to archive) instead of the wrong
        // `Some(ExactAgg(Sum))` / `Some(CardinalityApprox))` these used
        // to produce, which claimed the ASAP tier could answer a
        // `deriv()`/`changes()` query it structurally cannot.
        "delta" => Ok(AggFunc::Delta),
        "idelta" => Ok(AggFunc::IDelta),
        "deriv" => Ok(AggFunc::Deriv),
        "changes" => Ok(AggFunc::Changes),
        "resets" => Ok(AggFunc::Resets),
        // `Rate`/`Increase` now map onto their own dedicated `AggIntent`s
        // in `lower.rs` (`capability_for` already has a real, tested
        // `Rate | Increase => ExactAgg(Increase)` arm — this activates
        // it for the first time via the PromQL path; previously
        // collapsed onto `AggIntent::Sum`, which happened to route to
        // the same *kind* of exact-precompute capability but under the
        // wrong classification).
        "rate" | "irate" => Ok(AggFunc::Rate { window }),
        "increase" => Ok(AggFunc::Increase { window }),
        other => Err(anyhow!("unsupported PromQL function: {other}")),
    }
}

/// Short lowercase label for an `AggFunc`, used as the `AggItem` alias.
/// `AggFunc` is foreign (from `asap_l2`) — Rust's orphan rules forbid
/// implementing `Display` for it here, unlike this repo's pre-merge own
/// `AggFunc`, which had one.
fn agg_func_label(f: &AggFunc) -> String {
    match f {
        AggFunc::Count => "count".into(),
        AggFunc::Sum => "sum".into(),
        AggFunc::Avg => "avg".into(),
        AggFunc::Min => "min".into(),
        AggFunc::Max => "max".into(),
        AggFunc::StdDev { .. } => "stddev".into(),
        AggFunc::Variance { .. } => "variance".into(),
        AggFunc::Quantile(_) => "quantile".into(),
        AggFunc::CountDistinct => "count_distinct".into(),
        AggFunc::HeavyHitters { .. } => "heavy_hitters".into(),
        AggFunc::Rate { .. } => "rate".into(),
        AggFunc::Increase { .. } => "increase".into(),
        AggFunc::Delta => "delta".into(),
        other => format!("{other:?}").to_lowercase(),
    }
}

/// Build a Layer 2 `QueryExpr`: `Aggregate { AggFunc, input: Window { ... } }`.
fn build_qe_aggregate(
    metric: String,
    filters: Vec<Predicate>,
    window: std::time::Duration,
    func: AggFunc,
    ctx: WalkCtx,
) -> QueryExpr {
    let source = QueryExpr::Source(QeSourceSpec::new(metric));
    let filtered = apply_qe_filters(source, filters);
    let windowed = QueryExpr::Window {
        duration: window,
        slide: None,
        input: Box::new(filtered),
    };
    let actual_func = if ctx.topk.is_some() {
        // Inside topk context, the aggregation is frequency-based.
        AggFunc::Count
    } else {
        func
    };
    // Propagate partition keys into the Aggregate's GROUP BY so
    // `lower.rs`'s `agg_func_to_intents` sees a grouped `Count` → the
    // `Frequency` sketch trigger (not a bare, exact `Count`).
    let (group_keys, without): (Vec<String>, bool) = match &ctx.partition {
        Some(GroupMod::By(k)) => (k.clone(), false),
        Some(GroupMod::Without(k)) => (k.clone(), true),
        None => (Vec::new(), false),
    };
    let alias = agg_func_label(&actual_func);
    QueryExpr::Aggregate {
        keys: group_keys.into_iter().map(QeColumnRef::Named).collect(),
        without,
        aggs: vec![AggItem {
            alias: Some(alias),
            func: actual_func,
            col: QeColumnRef::SampleValue,
        }],
        having: None,
        input: Box::new(windowed),
    }
}

fn apply_qe_filters(input: QueryExpr, filters: Vec<Predicate>) -> QueryExpr {
    if filters.is_empty() {
        return input;
    }
    use crate::intent_algebra::{L2Expr, L3Scalar};

    let conjuncts: Vec<L2Expr> = filters
        .iter()
        .map(|p| {
            let col = L2Expr::Column(QeColumnRef::Named(p.col.clone()));
            let val = |v: &FilterVal| match v {
                FilterVal::Str(s) => L2Expr::Literal(L3Scalar::Utf8(s.clone())),
                FilterVal::Num(n) => L2Expr::Literal(L3Scalar::Float64(*n)),
                FilterVal::Int(i) => L2Expr::Literal(L3Scalar::Int64(*i)),
                FilterVal::Null => L2Expr::Literal(L3Scalar::Null),
            };
            match &p.op {
                FilterOp::Eq => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Eq,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::Ne => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Ne,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::Lt => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Lt,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::Le => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Le,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::Gt => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Gt,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::Ge => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Ge,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::Regex(r) => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Regex,
                    right: Box::new(L2Expr::Literal(L3Scalar::Utf8(r.clone()))),
                },
                FilterOp::NotRegex(r) => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::NotRegex,
                    right: Box::new(L2Expr::Literal(L3Scalar::Utf8(r.clone()))),
                },
                FilterOp::Like => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::Like,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::NotLike => L2Expr::Compare {
                    left: Box::new(col),
                    op: CompareOp::NotLike,
                    right: Box::new(val(&p.val)),
                },
                FilterOp::IsNull => L2Expr::IsNull(Box::new(col)),
                FilterOp::IsNotNull => L2Expr::IsNotNull(Box::new(col)),
            }
        })
        .collect();
    let pred = if conjuncts.len() == 1 {
        conjuncts.into_iter().next().unwrap()
    } else {
        L2Expr::BoolAnd(conjuncts)
    };
    QueryExpr::Filter {
        pred,
        input: Box::new(input),
    }
}

/// Fold `group` into the nearest `Aggregate` inside `qe` — `asap_l2`'s
/// `Aggregate` carries `keys`/`without` directly (no separate `Partition`
/// node to wrap in; see `intent_algebra::relational`'s module doc).
/// Mirrors `lower.rs`'s `fold_partition_keys`, one layer up (L2, not L3):
/// handles the shapes the walker actually produces (a bare `Aggregate` or
/// a `Window` wrapping one); anything else (`BinaryOp`, a bare `Source`)
/// has no `Aggregate` to fold into and passes through unchanged — e.g.
/// `sum by (host) (a or b)`, where the group modifier belongs to a
/// `BinaryOp` composition, not a reducing aggregate.
fn fold_group_mod(qe: QueryExpr, group: Option<&GroupMod>) -> QueryExpr {
    let Some(group) = group else {
        return qe;
    };
    if group.is_empty() {
        return qe;
    }
    match qe {
        QueryExpr::Aggregate {
            aggs,
            having,
            input,
            ..
        } => QueryExpr::Aggregate {
            keys: group
                .keys()
                .iter()
                .cloned()
                .map(QeColumnRef::Named)
                .collect(),
            without: matches!(group, GroupMod::Without(_)),
            aggs,
            having,
            input,
        },
        QueryExpr::Window {
            duration,
            slide,
            input,
        } => QueryExpr::Window {
            duration,
            slide,
            input: Box::new(fold_group_mod(*input, Some(group))),
        },
        other => other,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::types::AggType;
    use std::time::Duration;

    fn pq(q: &str) -> super::super::ParsedQuery {
        super::super::parse_query(q)
            .unwrap_or_else(|e| panic!("parse_query failed: {e}\nquery={q:?}"))
    }

    // ── quantile_over_time ────────────────────────────────────────────────────

    #[test]
    fn quantile_over_time_basic() {
        // PromQL: `by` is part of the aggregate operator, not the function call.
        let pq = pq("sum by (host) (quantile_over_time(0.99, latency{service=\"web\"}[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.99]);
        assert_eq!(pq.group_by_labels, vec!["host"]);
        assert_eq!(
            pq.label_filters.get("service").map(String::as_str),
            Some("web")
        );
        assert_eq!(pq.time_window, Duration::from_secs(300));
    }

    #[test]
    fn quantile_over_time_debs_ema() {
        // Dotted names are invalid PromQL; use underscores.
        let pq = pq("sum by (symbol) (quantile_over_time(0.5, financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.5]);
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
    }

    // ── histogram_quantile ────────────────────────────────────────────────────

    #[test]
    fn histogram_quantile_via_rate() {
        let pq = pq("histogram_quantile(0.95, rate(http_duration_seconds_bucket[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.95]);
    }

    /// Step γ5 contract: at the PromQL parser level, `histogram_quantile(φ, …)`
    /// is substituted into a plain `QueryExpr::Aggregate { aggs: [AggItem {
    /// func: AggFunc::Quantile(φ), … }], … }` so downstream code sees a
    /// single canonical Quantile intent (no `QueryExpr::HistogramQuantile`
    /// wrapper). `ParsedQuery.quantiles` records the φ via the existing
    /// multi-quantile machinery.
    #[test]
    fn histogram_quantile_lowers_to_plain_aggregate_quantile() {
        use crate::intent_algebra::relational::{AggFunc, ColumnRef, QueryExpr};

        let qe = super::parse_promql_expr(
            r#"histogram_quantile(0.99, rate(http_requests_bucket{le="0.5"}[5m]))"#,
        )
        .expect("parse should succeed");

        // The top of the tree must be a plain Aggregate with a single
        // Quantile(0.99) AggItem — NOT a HistogramQuantile wrapper.
        match &qe {
            QueryExpr::Aggregate { aggs, .. } => {
                assert_eq!(aggs.len(), 1, "expected single AggItem, got {aggs:?}");
                let item = &aggs[0];
                match item.func {
                    AggFunc::Quantile(phi) => {
                        assert!((phi - 0.99).abs() < 1e-9, "expected φ=0.99, got {phi}");
                    }
                    ref other => panic!("expected AggFunc::Quantile(0.99), got {other:?}"),
                }
                assert!(matches!(item.col, ColumnRef::SampleValue));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }

        // The flat ParsedQuery view exposes the φ via the existing
        // multi-quantile machinery.
        let pq = pq(r#"histogram_quantile(0.99, rate(http_requests_bucket{le="0.5"}[5m]))"#);
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.99]);
        assert_eq!(pq.label_filters.get("le").map(String::as_str), Some("0.5"),);
    }

    // ── avg_over_time ─────────────────────────────────────────────────────────

    #[test]
    fn avg_over_time_maps_to_p50() {
        let pq = pq("avg by (symbol) (avg_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.5]);
    }

    // ── min/max_over_time ─────────────────────────────────────────────────────

    #[test]
    fn min_over_time_with_by_is_ddsketch() {
        let pq = pq("min by (symbol) (min_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.0]);
    }

    #[test]
    fn max_over_time_with_by_is_ddsketch() {
        let pq = pq("max by (symbol) (max_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![1.0]);
    }

    // ── topk ──────────────────────────────────────────────────────────────────

    #[test]
    fn topk_count_over_time() {
        let pq = pq("topk by (symbol) (10, count_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
        assert_eq!(pq.group_by_labels, vec!["symbol"]);
    }

    #[test]
    fn topk_avg_over_time() {
        // `topk` ranking by a non-count measure (`avg_over_time`, here)
        // is not a heavy-hitter shape — `RankingMeasure::NonAdditive`,
        // per `agg_intent::is_frequency_heavy_hitter` — so this becomes
        // a generic `Sort + Limit` over `avg_over_time`'s own
        // `Aggregate{Quantile(0.5)}` p50 approximation, not a forced
        // `Count`/`Frequency` aggregate. Before the topk/rate precision
        // fix this incorrectly asserted `[Frequency]`.
        let pq = pq("topk by (host) (5, avg_over_time(cpu[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.5]);
    }

    #[test]
    fn topk_avg_over_time_is_sort_limit_not_topk_node() {
        // Structural check backing `topk_avg_over_time` above: the tree
        // must be `Limit { Sort { Aggregate{Quantile} } }`, not
        // `QueryExpr::TopK` — confirms the non-heavy-hitter path is
        // really taken, not just that the flattened `ParsedQuery`
        // happens to read the same.
        use crate::intent_algebra::relational::QueryExpr;

        let qe = super::parse_promql_expr("topk by (host) (5, avg_over_time(cpu[5m]))")
            .expect("parse should succeed");
        match &qe {
            QueryExpr::Limit { n, input, .. } => {
                assert_eq!(*n, 5);
                assert!(
                    matches!(input.as_ref(), QueryExpr::Sort { .. }),
                    "expected Sort under Limit, got {input:?}"
                );
            }
            other => panic!("expected Limit{{Sort{{...}}}}, got {other:?}"),
        }
    }

    #[test]
    fn bottomk_count_over_time_is_never_heavy_hitter() {
        // `bottomk` never qualifies as the heavy-hitter `TopK` intent
        // even when ranking by `count_over_time` — `descending` must
        // also hold (`is_frequency_heavy_hitter`), and `bottomk` is
        // ascending by definition. Must still lower to `Limit{Sort{...}}`.
        use crate::intent_algebra::relational::QueryExpr;

        let qe = super::parse_promql_expr("bottomk(5, count_over_time(http_requests_total[5m]))")
            .expect("parse should succeed");
        assert!(
            !matches!(qe, QueryExpr::TopK { .. }),
            "bottomk must never produce the heavy-hitter TopK node, got {qe:?}"
        );
        match &qe {
            QueryExpr::Limit { n, input, .. } => {
                assert_eq!(*n, 5);
                match input.as_ref() {
                    QueryExpr::Sort { keys, .. } => {
                        assert!(keys[0].ascending, "bottomk must sort ascending");
                    }
                    other => panic!("expected Sort under Limit, got {other:?}"),
                }
            }
            other => panic!("expected Limit{{Sort{{...}}}}, got {other:?}"),
        }
    }

    #[test]
    fn topk_count_over_time_is_topk_node() {
        // The one real heavy-hitter shape: `topk` (descending) ranking
        // by `count_over_time` (`RankingMeasure::Frequency`).
        use crate::intent_algebra::relational::QueryExpr;

        let qe = super::parse_promql_expr("topk(5, count_over_time(http_requests_total[5m]))")
            .expect("parse should succeed");
        assert!(
            matches!(qe, QueryExpr::TopK { k: 5, .. }),
            "expected QueryExpr::TopK{{k: 5, ..}}, got {qe:?}"
        );
    }

    // ── count cardinality ─────────────────────────────────────────────────────

    #[test]
    fn count_count_over_time_is_hll() {
        let pq = pq("count by (symbol) (count_over_time(financial_last_trade_price[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Cardinality]);
    }

    // ── stddev_over_time ──────────────────────────────────────────────────────

    #[test]
    fn stddev_over_time_iqr_proxy() {
        let pq = pq("avg by (host) (stddev_over_time(cpu[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert!(pq.quantiles.contains(&0.25) && pq.quantiles.contains(&0.75));
    }

    // ── sum_over_time → exact ─────────────────────────────────────────────────

    #[test]
    fn sum_over_time_exact() {
        let pq = pq("sum by (service) (sum_over_time(request_bytes[1h]))");
        assert!(pq.exact_required);
    }

    // ── label filters ─────────────────────────────────────────────────────────

    #[test]
    fn label_eq_filter() {
        let pq = pq(r#"sum by (service) (count_over_time(hits{env="prod"}[5m]))"#);
        assert_eq!(
            pq.label_filters.get("env").map(String::as_str),
            Some("prod")
        );
    }

    // ── duration parsing ──────────────────────────────────────────────────────

    #[test]
    fn duration_1h() {
        let pq = pq("avg by (host) (avg_over_time(cpu[1h]))");
        assert_eq!(pq.time_window, Duration::from_secs(3600));
    }

    // ── DEBS hints ────────────────────────────────────────────────────────────

    #[test]
    fn debs_price_stats_min() {
        use super::super::QueryHint;
        let pq = pq("min by (symbol) (min_over_time(financial_last_trade_price[5m]))");
        assert!(matches!(pq.hint, Some(QueryHint::DebsPriceStats)));
    }

    #[test]
    fn debs_cardinality() {
        use super::super::QueryHint;
        let pq = pq("count by (symbol) (count_over_time(financial_last_trade_price[5m]))");
        assert!(matches!(pq.hint, Some(QueryHint::DebsCardinality)));
    }

    // ── Complex queries ───────────────────────────────────────────────────────

    #[test]
    fn complex_topk_count_over_time_multi_label() {
        // topk absorbs CountSketch (R8); multiple label filters extracted
        let pq = pq(
            r#"topk by (service) (10, count_over_time(http_requests_total{status="500",env="prod"}[5m]))"#,
        );
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
        assert_eq!(pq.group_by_labels, vec!["service"]);
        assert_eq!(
            pq.label_filters.get("status").map(String::as_str),
            Some("500")
        );
        assert_eq!(
            pq.label_filters.get("env").map(String::as_str),
            Some("prod")
        );
        assert_eq!(pq.time_window, Duration::from_secs(300));
    }

    #[test]
    fn complex_histogram_quantile_multi_label() {
        // histogram_quantile wraps rate → DDSketch; two label selectors
        let pq = pq(
            r#"histogram_quantile(0.99, rate(request_duration_seconds_bucket{service="checkout",region="us-east"}[10m]))"#,
        );
        assert_eq!(pq.aggregations, vec![AggType::Quantile]);
        assert_eq!(pq.quantiles, vec![0.99]);
        assert_eq!(
            pq.label_filters.get("service").map(String::as_str),
            Some("checkout")
        );
        assert_eq!(
            pq.label_filters.get("region").map(String::as_str),
            Some("us-east")
        );
        assert_eq!(pq.time_window, Duration::from_secs(600));
    }
}
