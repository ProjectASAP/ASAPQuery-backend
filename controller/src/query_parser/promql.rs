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
//! | `histogram_quantile(φ, rate(m[w]))` | (HistogramQuantile node — not an Aggregate) |
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
use promql_parser::parser::{self, AggregateExpr, Call, Expr, LabelModifier, MatrixSelector, VectorSelector};

use crate::algebra::expr::{FilterOp, FilterVal, PartitionKeys, Predicate};

// ── Walk context ──────────────────────────────────────────────────────────────

/// Context accumulated as we descend the AST.
#[derive(Default, Clone)]
struct WalkCtx {
    /// GROUP BY / `without` clause from an outer Aggregate node.
    partition: Option<PartitionKeys>,
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
    let arg = call.args.args.get(arg_idx)
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
        other => Err(anyhow!("expected MatrixSelector, got {:?}", std::mem::discriminant(other))),
    }
}

// ── Helpers: VectorSelector info ─────────────────────────────────────────────

fn extract_vs_info(vs: &VectorSelector) -> (String, Vec<Predicate>) {
    // Metric name: prefer the explicit name field, fall back to __name__ matcher.
    let name = vs.name.clone().unwrap_or_else(|| {
        vs.matchers.matchers.iter()
            .find(|m| m.name == "__name__")
            .map(|m| m.value.clone())
            .unwrap_or_default()
    });

    let filters = vs.matchers.matchers.iter()
        .filter(|m| m.name != "__name__")
        .filter_map(matcher_to_predicate)
        .collect();

    (name, filters)
}

fn matcher_to_predicate(m: &promql_parser::label::Matcher) -> Option<Predicate> {
    use promql_parser::label::MatchOp;
    let (op, val) = match &m.op {
        MatchOp::Equal    => (FilterOp::Eq, FilterVal::Str(m.value.clone())),
        MatchOp::NotEqual => (FilterOp::Ne, FilterVal::Str(m.value.clone())),
        MatchOp::Re(re)   => (FilterOp::Regex(re.to_string()), FilterVal::Str(m.value.clone())),
        MatchOp::NotRe(re)=> (FilterOp::NotRegex(re.to_string()), FilterVal::Str(m.value.clone())),
    };
    Some(Predicate { col: m.name.clone(), op, val })
}

// ── Helpers: number extraction ────────────────────────────────────────────────

fn extract_call_num_arg(call: &Call, idx: usize) -> anyhow::Result<f64> {
    match call.args.args.get(idx).map(|b| b.as_ref()) {
        Some(Expr::NumberLiteral(n)) => Ok(n.val),
        Some(other) => Err(anyhow!(
            "expected number at arg {} of {}, got {:?}",
            idx, call.func.name, std::mem::discriminant(other)
        )),
        None => Err(anyhow!("missing arg {} in {}", idx, call.func.name)),
    }
}

fn extract_number_param(param: &Option<Box<Expr>>) -> anyhow::Result<f64> {
    match param {
        Some(e) => match e.as_ref() {
            Expr::NumberLiteral(n) => Ok(n.val),
            other => Err(anyhow!("expected number param, got {:?}", std::mem::discriminant(other))),
        },
        None => Err(anyhow!("missing required numeric parameter")),
    }
}

// ── Helpers: PartitionKeys from LabelModifier ─────────────────────────────────

fn modifier_to_partition(modifier: &LabelModifier) -> PartitionKeys {
    match modifier {
        LabelModifier::Include(labels) => PartitionKeys::By(labels.labels.clone()),
        LabelModifier::Exclude(labels) => PartitionKeys::Without(labels.labels.clone()),
    }
}

// ── Direct QueryExpr emission ─────────────────────────────────────────────────
//
// `parse_promql_expr` walks the same PromQL AST but emits [`QueryExpr`] nodes
// natively, preserving semantic nodes for the algebra optimizer:
//
// | PromQL pattern           | QueryExpr node                        |
// |--------------------------|---------------------------------------|
// | `histogram_quantile(φ…)` | HistogramQuantile { phi }             |
// | `m[5m:1m]` subquery      | PromQLSubquery { 5m, Some(1m) }      |
// | `a op b` binary          | BinaryOp { VectorMatch }             |

use crate::algebra::expr::{
    AggFunc, AggItem,
    BinaryOpKind, ColumnRef as QeColumnRef, GroupSide,
    PartitionKeys as QePartitionKeys, QueryExpr,
    SourceSpec as QeSourceSpec, VectorGrouping, VectorMatch, VectorMatchKind,
};
use promql_parser::parser::{token::TokenType, BinaryExpr, VectorMatchCardinality};

/// Parse a PromQL expression string directly into an optimised [`QueryExpr`].
///
/// This preserves
/// `HistogramQuantile`, `PromQLSubquery`, and `BinaryOp` nodes natively.
pub fn parse_promql_expr(query: &str) -> anyhow::Result<QueryExpr> {
    let expr = parser::parse(query)
        .map_err(|e| anyhow!("PromQL parse error: {e}"))?;
    walk_qe(&expr, WalkCtx::default())
}

fn walk_qe(expr: &Expr, ctx: WalkCtx) -> anyhow::Result<QueryExpr> {
    match expr {
        Expr::Aggregate(agg) => walk_aggregate_qe(agg, ctx),
        Expr::Call(call)     => walk_call_qe(call, ctx),

        // Binary op: map to QueryExpr::BinaryOp with VectorMatch.
        Expr::Binary(bin) => walk_binary_qe(bin),

        Expr::Paren(p) => walk_qe(p.expr.as_ref(), ctx),

        // Subquery `expr[range:resolution]` → PromQLSubquery.
        Expr::Subquery(sq) => {
            let inner = walk_qe(sq.expr.as_ref(), ctx.clone())?;
            Ok(QueryExpr::PromQLSubquery {
                range:      sq.range,
                resolution: sq.step,
                input:      Box::new(inner),
            })
        }

        // Bare vector selector → Source + Filter + Aggregate(Sum).
        Expr::VectorSelector(vs) => {
            let (name, filters) = extract_vs_info(vs);
            let source   = QueryExpr::Source(QeSourceSpec { name });
            let filtered = apply_qe_filters(source, filters);
            Ok(QueryExpr::Aggregate {
                keys:   vec![],
                aggs:   vec![AggItem {
                    alias:    "value".into(),
                    func:     AggFunc::Sum,
                    col:      QeColumnRef::SampleValue,
                    distinct: false,
                }],
                having: None,
                input:  Box::new(filtered),
            })
        }

        Expr::NumberLiteral(_) | Expr::StringLiteral(_) =>
            Err(anyhow!("unexpected literal at top level of PromQL expression")),

        #[allow(unreachable_patterns)]
        _ => Err(anyhow!("unsupported PromQL expression type")),
    }
}

fn walk_aggregate_qe(agg: &AggregateExpr, ctx: WalkCtx) -> anyhow::Result<QueryExpr> {
    let partition = agg.modifier.as_ref().map(modifier_to_partition);
    let op_name   = format!("{}", agg.op);

    match op_name.as_str() {
        "topk" | "bottomk" => {
            let k = extract_number_param(&agg.param)? as u64;
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: Some(k), outer_count: false };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            // Don't wrap with Partition here — inner Aggregate already has the keys,
            // and the lowering pass will create the Partition when it lowers the Aggregate.
            let result = QueryExpr::TopK { k, by: partition.as_ref().map(|p| p.keys().to_vec()).unwrap_or_default(), input: Box::new(inner) };
            Ok(result)
        }
        "count" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: true };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys:   vec![],
                aggs:   vec![AggItem {
                    alias:    "count".into(),
                    func:     AggFunc::CountDistinct,
                    col:      QeColumnRef::SampleValue,
                    distinct: false,
                }],
                having: None,
                input:  Box::new(inner),
            };
            Ok(apply_qe_partition(result, partition))
        }
        "sum" | "avg" | "min" | "max" | "group" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: ctx.topk, outer_count: false };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            Ok(apply_qe_partition(inner, partition))
        }
        "stddev" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: false };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys:   vec![],
                aggs:   vec![AggItem {
                    alias:    "stddev".into(),
                    func:     AggFunc::StdDev { population: false },
                    col:      QeColumnRef::SampleValue,
                    distinct: false,
                }],
                having: None,
                input:  Box::new(inner),
            };
            Ok(apply_qe_partition(result, partition))
        }
        "stdvar" => {
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: false };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys:   vec![],
                aggs:   vec![AggItem {
                    alias:    "stdvar".into(),
                    func:     AggFunc::Variance { population: false },
                    col:      QeColumnRef::SampleValue,
                    distinct: false,
                }],
                having: None,
                input:  Box::new(inner),
            };
            Ok(apply_qe_partition(result, partition))
        }
        "quantile" => {
            let phi = extract_number_param(&agg.param)?;
            let inner_ctx = WalkCtx { partition: partition.clone(), topk: None, outer_count: false };
            let inner = walk_qe(agg.expr.as_ref(), inner_ctx)?;
            let result = QueryExpr::Aggregate {
                keys:   vec![],
                aggs:   vec![AggItem {
                    alias:    "quantile".into(),
                    func:     AggFunc::Quantile(phi),
                    col:      QeColumnRef::SampleValue,
                    distinct: false,
                }],
                having: None,
                input:  Box::new(inner),
            };
            Ok(apply_qe_partition(result, partition))
        }
        other => Err(anyhow!("unsupported PromQL aggregate operator: {other}")),
    }
}

fn walk_call_qe(call: &Call, ctx: WalkCtx) -> anyhow::Result<QueryExpr> {
    let name = call.func.name;
    match name {
        // histogram_quantile → native HistogramQuantile node (PromQL-specific).
        "histogram_quantile" => {
            let phi       = extract_call_num_arg(call, 0)?;
            let rate_expr = call.args.args[1].as_ref();
            let (source, filters, window) = extract_inner_matrix(rate_expr)?;
            let inner = build_qe_aggregate(source, filters, window,
                AggFunc::Quantile(phi),
                WalkCtx::default());
            Ok(QueryExpr::HistogramQuantile { phi, input: Box::new(inner) })
        }
        // All other function calls: map to AggFunc (Layer 2).
        "quantile_over_time" => {
            let phi = extract_call_num_arg(call, 0)?;
            let (source, filters, window) = extract_matrix_arg(call, 1)?;
            let func = AggFunc::Quantile(phi);
            Ok(build_qe_aggregate(source, filters, window, func, ctx))
        }
        _ => {
            let func = walk_call_to_op(call, &ctx)?;
            let (source, filters, window) = if call.func.name == "rate"
                || call.func.name == "irate"
                || call.func.name == "increase"
            {
                let arg = call.args.args.first()
                    .map(|b| b.as_ref())
                    .ok_or_else(|| anyhow!("rate/irate/increase requires a matrix arg"))?;
                extract_inner_matrix(arg)?
            } else {
                extract_matrix_arg(call, 0)?
            };
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
            Some(LabelModifier::Include(ls)) => (VectorMatchKind::On,       ls.labels.clone()),
            Some(LabelModifier::Exclude(ls)) => (VectorMatchKind::Ignoring, ls.labels.clone()),
            None                              => (VectorMatchKind::On,       vec![]),
        };
        let grouping = match &m.card {
            VectorMatchCardinality::ManyToOne(ls) => Some(VectorGrouping {
                side:   GroupSide::Left,
                labels: ls.labels.clone(),
            }),
            VectorMatchCardinality::OneToMany(ls) => Some(VectorGrouping {
                side:   GroupSide::Right,
                labels: ls.labels.clone(),
            }),
            _ => None,
        };
        VectorMatch { kind, labels, grouping }
    });

    Ok(QueryExpr::BinaryOp {
        op,
        lhs:          Box::new(lhs),
        rhs:          Box::new(rhs),
        vector_match,
    })
}

fn promql_token_to_binop(tok: TokenType) -> BinaryOpKind {
    use promql_parser::parser::token;
    // token::T_* are u8 constants; TokenType wraps them as TokenType(u8).
    let id = tok.id();
    match id {
        token::T_ADD     => BinaryOpKind::Add,
        token::T_SUB     => BinaryOpKind::Sub,
        token::T_MUL     => BinaryOpKind::Mul,
        token::T_DIV     => BinaryOpKind::Div,
        token::T_MOD     => BinaryOpKind::Mod,
        token::T_POW     => BinaryOpKind::Pow,
        token::T_EQLC    => BinaryOpKind::Eq,
        token::T_NEQ     => BinaryOpKind::Ne,
        token::T_LSS     => BinaryOpKind::Lt,
        token::T_LTE     => BinaryOpKind::Le,
        token::T_GTR     => BinaryOpKind::Gt,
        token::T_GTE     => BinaryOpKind::Ge,
        token::T_LAND    => BinaryOpKind::And,
        token::T_LOR     => BinaryOpKind::Or,
        token::T_LUNLESS => BinaryOpKind::Unless,
        token::T_ATAN2   => BinaryOpKind::Atan2,
        _                => BinaryOpKind::Add, // unknown — default to add
    }
}

/// Map a PromQL function call to an [`AggFunc`] (Layer 2 relational operator).
fn walk_call_to_op(call: &Call, ctx: &WalkCtx) -> anyhow::Result<AggFunc> {
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
            Ok(if ctx.outer_count { AggFunc::CountDistinct } else { AggFunc::Count })
        }
        "sum_over_time" | "last_over_time" | "present_over_time" | "absent_over_time" =>
            Ok(AggFunc::Sum),
        "delta" | "idelta" | "deriv" | "predict_linear" =>
            Ok(AggFunc::Delta),
        "changes" | "resets" => Ok(AggFunc::Count),
        "rate" | "irate" => Ok(AggFunc::Rate),
        "increase" => Ok(AggFunc::Increase),
        other => Err(anyhow!("unsupported PromQL function: {other}")),
    }
}

/// Build a Layer 2 `QueryExpr`: `Aggregate { AggFunc, input: Window { ... } }`.
fn build_qe_aggregate(
    metric:  String,
    filters: Vec<Predicate>,
    window:  std::time::Duration,
    func:    AggFunc,
    ctx:     WalkCtx,
) -> QueryExpr {
    let source   = QueryExpr::Source(QeSourceSpec { name: metric });
    let filtered = apply_qe_filters(source, filters);
    let windowed = QueryExpr::Window {
        duration: window,
        slide:    None,
        input:    Box::new(filtered),
    };
    let actual_func = if ctx.topk.is_some() {
        // Inside topk context, the aggregation is frequency-based.
        AggFunc::Count
    } else {
        func
    };
    // Propagate partition keys into the Aggregate's GROUP BY so the lowering
    // pass sees Count-with-GROUP-BY → Frequency (not bare Count → no sketch).
    let group_keys: Vec<String> = ctx.partition.as_ref()
        .map(|p| p.keys().to_vec())
        .unwrap_or_default();
    let alias = format!("{}", actual_func).to_lowercase();
    let agg = QueryExpr::Aggregate {
        keys:   group_keys,
        aggs:   vec![AggItem {
            alias,
            func:     actual_func,
            col:      QeColumnRef::SampleValue,
            distinct: false,
        }],
        having: None,
        input:  Box::new(windowed),
    };
    // Don't wrap with Partition separately — keys are already in the Aggregate.
    // The lowering pass will create the Partition node when it lowers the Aggregate.
    agg
}

fn apply_qe_filters(
    input:   QueryExpr,
    filters: Vec<Predicate>,
) -> QueryExpr {
    if filters.is_empty() {
        input
    } else {
        use crate::algebra::expr::{BinaryOpKind, LiteralValue, ScalarExpr};
        let pred = filters.iter().fold(
            ScalarExpr::Literal(LiteralValue::Bool(true)),
            |acc, p| {
                let col = ScalarExpr::Column(p.col.clone());
                let val = match &p.val {
                    FilterVal::Str(s)  => ScalarExpr::Literal(LiteralValue::Str(s.clone())),
                    FilterVal::Num(n)  => ScalarExpr::Literal(LiteralValue::Float(*n)),
                    FilterVal::Int(i)  => ScalarExpr::Literal(LiteralValue::Int(*i)),
                    FilterVal::Null    => ScalarExpr::Literal(LiteralValue::Null),
                };
                let this = match &p.op {
                    FilterOp::Eq       => ScalarExpr::BinaryOp { op: BinaryOpKind::Eq,       lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::Ne       => ScalarExpr::BinaryOp { op: BinaryOpKind::Ne,       lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::Lt       => ScalarExpr::BinaryOp { op: BinaryOpKind::Lt,       lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::Le       => ScalarExpr::BinaryOp { op: BinaryOpKind::Le,       lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::Gt       => ScalarExpr::BinaryOp { op: BinaryOpKind::Gt,       lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::Ge       => ScalarExpr::BinaryOp { op: BinaryOpKind::Ge,       lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::Regex(r) => ScalarExpr::BinaryOp { op: BinaryOpKind::Regex,    lhs: Box::new(col), rhs: Box::new(ScalarExpr::Literal(LiteralValue::Str(r.clone()))) },
                    FilterOp::NotRegex(r) => ScalarExpr::BinaryOp { op: BinaryOpKind::NotRegex, lhs: Box::new(col), rhs: Box::new(ScalarExpr::Literal(LiteralValue::Str(r.clone()))) },
                    FilterOp::Like     => ScalarExpr::BinaryOp { op: BinaryOpKind::Like,     lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::NotLike  => ScalarExpr::BinaryOp { op: BinaryOpKind::NotLike,  lhs: Box::new(col), rhs: Box::new(val) },
                    FilterOp::IsNull   => ScalarExpr::IsNull { expr: Box::new(col), negated: false },
                    FilterOp::IsNotNull => ScalarExpr::IsNull { expr: Box::new(col), negated: true },
                };
                ScalarExpr::BinaryOp {
                    op:  BinaryOpKind::And,
                    lhs: Box::new(acc),
                    rhs: Box::new(this),
                }
            },
        );
        QueryExpr::Filter { pred, input: Box::new(input) }
    }
}

fn apply_qe_partition(
    input:     QueryExpr,
    partition: Option<PartitionKeys>,
) -> QueryExpr {
    match partition {
        None => input,
        Some(p) if p.is_empty() => input,
        Some(keys) => {
            // Convert PromQL by/without → PartitionKeys.
            let qe_keys = match keys {
                PartitionKeys::By(k)      => QePartitionKeys::By(k),
                PartitionKeys::Without(k) => QePartitionKeys::Without(k),
            };
            QueryExpr::Partition { keys: qe_keys, input: Box::new(input) }
        }
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
        assert_eq!(pq.label_filters.get("service").map(String::as_str), Some("web"));
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
        let pq = pq("topk by (host) (5, avg_over_time(cpu[5m]))");
        assert_eq!(pq.aggregations, vec![AggType::Frequency]);
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
        assert_eq!(pq.label_filters.get("env").map(String::as_str), Some("prod"));
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
