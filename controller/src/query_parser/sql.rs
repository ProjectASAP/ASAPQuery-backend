//! Layers 1→3 lowering: SQL string → QueryExpr (sketch logical plan).
//!
//! - **Layer 1**: the `sqlparser` crate parses the SQL string into a
//!   language-specific AST (`sqlparser::ast::Statement`).
//! - **Layer 2**: the extraction functions (`extract_query_expr`, `extract_select_qe`)
//!   interpret SQL semantics (SELECT projection, GROUP BY, WHERE, JOIN, ORDER BY,
//!   LIMIT, UNION ALL) and lower them to the sketch algebra.
//! - **Layer 3**: the output is a `QueryExpr` tree with **relational operators only**
//!   (`Source`, `Filter`, `Aggregate`, `Join`, `Sort`, `Limit`, `SetOp`).
//!
//! # Key difference from the PromQL parser
//!
//! The SQL parser does **not** emit `SketchAgg` or `WindowedAgg` nodes.  It emits
//! generic `Aggregate { func: Avg/Count/CountDistinct/... }` nodes.  Sketch assignment
//! happens later:
//! - **Layer 4 (optimizer)**: R5 TopKFusion rewrites `Limit(Sort(Aggregate))` → `TopK`;
//!   R9 HydraConversion rewrites multi-key `CountDistinct` → `PerPartition`.
//! - **Layer 5 (physical planner / stage-split)**: `assign_agg_func` maps each `AggFunc`
//!   to an `AggIntent` (e.g., `CountDistinct` → `Cardinality`, `Quantile(φ)` → `Quantile`).
//!
//! This means the SQL path goes: relational plan → optimizer rewrites → physical
//! sketch assignment, whereas PromQL goes: sketch plan directly → optimizer → physical.
//!
//! # Algorithm
//!
//! For each FUNCTION edge in the SELECT projection (leaf → root):
//!   1. Collect context: GROUP BY, WHERE, HAVING, JOIN, DISTINCT, UNION ALL
//!   2. Emit the corresponding `QueryExpr` node:
//!      - WHERE predicates   → `Filter { ScalarExpr }`
//!      - GROUP BY + aggs    → `Aggregate { keys, aggs }`
//!      - ORDER BY + LIMIT   → `Sort` + `Limit` (→ `TopK` via optimizer R5)
//!      - JOIN … ON          → `Join { kind, pred }`
//!      - UNION ALL          → `SetOp { Union, all: true }`
//!
//! Multiple aggregations in one SELECT each emit their own `AggItem`,
//! collected inside a single `Aggregate` node.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context};
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, Join, JoinConstraint,
    JoinOperator, LimitClause, ObjectName, OrderBy, OrderByExpr, OrderByKind,
    Query, Select, SelectItem, SetExpr, SetOperator, Statement, TableFactor,
    Value, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;

use crate::intent_algebra::legacy_expr::{
    AggFunc, AggItem as AlgAggItem, BinaryOpKind, ColumnRef, JoinKind, LiteralValue,
    ProjectItem, QueryExpr, ScalarExpr, SetOpKind, SortKey, SourceSpec,
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Parse a SQL SELECT statement into a [`QueryExpr`] tree.
///
/// Preserves `Sort`, `Limit`, `Join`, and `SetOp` nodes natively so the
/// [`crate::algebra`] optimizer and allocator can reason about them.
pub fn parse_sql_expr(sql: &str) -> anyhow::Result<QueryExpr> {
    let dialect = GenericDialect {};
    let mut stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .with_context(|| format!("SQL parse error: {sql:?}"))?;
    let stmt = stmts.pop().ok_or_else(|| anyhow!("no SQL statement found"))?;
    let query = match stmt {
        Statement::Query(q) => *q,
        other => return Err(anyhow!("expected SELECT, got {:?}", other)),
    };
    extract_query_expr(&query)
}

// ── Query-level dispatch ──────────────────────────────────────────────────────

fn extract_query_expr(query: &Query) -> anyhow::Result<QueryExpr> {
    let order_by: Vec<OrderByExpr> = match &query.order_by {
        Some(OrderBy { kind: OrderByKind::Expressions(exprs), .. }) => exprs.clone(),
        _ => vec![],
    };
    let (limit_n, offset_n) = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit: Some(e), offset, .. }) => {
            (Some(e.clone()), offset.as_ref().and_then(|o| expr_to_u64(&o.value)))
        }
        Some(LimitClause::OffsetCommaLimit { limit: e, offset, .. }) => {
            (Some(e.clone()), Some(expr_to_u64(offset).unwrap_or(0)))
        }
        _ => (None, None),
    };
    let limit_val = limit_n.as_ref().and_then(|e| expr_to_u64(e));
    let offset_val = offset_n.unwrap_or(0);

    let body = extract_set_expr_qe(query.body.as_ref(), &order_by, limit_val, offset_val)?;
    Ok(body)
}

fn extract_set_expr_qe(
    set_expr:   &SetExpr,
    order_by:   &[OrderByExpr],
    limit_n:    Option<u64>,
    offset_n:   u64,
) -> anyhow::Result<QueryExpr> {
    match set_expr {
        SetExpr::Select(sel) => extract_select_qe(sel, order_by, limit_n, offset_n),
        SetExpr::Query(inner) => extract_query_expr(inner),

        // UNION / INTERSECT / EXCEPT
        SetExpr::SetOperation { left, right, op, set_quantifier } => {
            use sqlparser::ast::{SetOperator, SetQuantifier};
            let left_qe  = extract_set_expr_qe(left,  &[], None, 0)?;
            let right_qe = extract_set_expr_qe(right, &[], None, 0)?;
            let kind = match op {
                SetOperator::Union     => SetOpKind::Union,
                SetOperator::Intersect => SetOpKind::Intersect,
                SetOperator::Except | SetOperator::Minus => SetOpKind::Except,
            };
            let all = matches!(set_quantifier, SetQuantifier::All | SetQuantifier::ByName);
            Ok(QueryExpr::SetOp {
                kind,
                all,
                left:  Box::new(left_qe),
                right: Box::new(right_qe),
            })
        }
        other => Err(anyhow!("unsupported query body: {:?}", other)),
    }
}

// ── SELECT-level extraction ───────────────────────────────────────────────────

fn extract_select_qe(
    sel:      &Select,
    order_by: &[OrderByExpr],
    limit_n:  Option<u64>,
    offset_n: u64,
) -> anyhow::Result<QueryExpr> {
    let metric_name   = extract_table_name(sel)?;
    let where_scalar  = sel.selection.as_ref().map(sql_expr_to_scalar);
    let group_keys    = extract_group_by(&sel.group_by);
    let having_scalar = sel.having.as_ref().map(sql_expr_to_scalar);
    let agg_items     = collect_agg_items_qe(&sel.projection);
    let join_qe       = extract_join_qe(sel);
    let window_spec   = extract_group_by_window(&sel.group_by);

    let source = QueryExpr::Source(SourceSpec {
        name: metric_name.clone(),
    });

    // WHERE → Filter
    let after_where = match where_scalar {
        Some(pred) => QueryExpr::Filter { pred, input: Box::new(source) },
        None       => source,
    };

    // JOIN
    let after_join = if let Some((inner_table, join_kind, join_pred)) = join_qe {
        let inner_source = QueryExpr::Source(SourceSpec { name: inner_table });
        QueryExpr::Join {
            kind:  join_kind,
            pred:  join_pred,
            left:  Box::new(after_where),
            right: Box::new(inner_source),
        }
    } else {
        after_where
    };

    // TUMBLE / HOP → Window node wrapping the source
    let after_window = if let Some(ws) = window_spec {
        QueryExpr::Window {
            duration: ws.size,
            slide:    ws.slide,
            input:    Box::new(after_join),
        }
    } else {
        after_join
    };

    // GROUP BY + aggs OR bare projection
    let after_agg = if agg_items.is_empty() {
        // No aggregation — bare projection with possible DISTINCT.
        let cols = collect_project_items(&sel.projection);
        QueryExpr::Project { cols, input: Box::new(after_window) }
    } else {
        let having = having_scalar;
        QueryExpr::Aggregate {
            keys:   group_keys,
            aggs:   agg_items,
            having,
            input:  Box::new(after_window),
        }
    };

    // ORDER BY → Sort
    let after_sort = if order_by.is_empty() {
        after_agg
    } else {
        let keys: Vec<SortKey> = order_by.iter().map(|o| SortKey {
            col:         expr_to_col_name(&o.expr).unwrap_or_else(|| "?".into()),
            desc:        matches!(o.options.asc, Some(false) | None),
            nulls_first: None,
        }).collect();
        QueryExpr::Sort { keys, input: Box::new(after_agg) }
    };

    // LIMIT / OFFSET
    let result = match limit_n {
        Some(n) => QueryExpr::Limit { n, offset: offset_n, input: Box::new(after_sort) },
        None    => after_sort,
    };

    Ok(result)
}

// ── Aggregation item collection ──────────────────────────────────────────────

fn collect_agg_items_qe(projection: &[SelectItem]) -> Vec<AlgAggItem> {
    let mut out = Vec::new();
    for item in projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e)               => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _                                         => continue,
        };
        collect_agg_from_expr_qe(expr, alias, &mut out);
    }
    out
}

fn collect_agg_from_expr_qe(expr: &Expr, alias: Option<String>, out: &mut Vec<AlgAggItem>) {
    match expr {
        Expr::Function(f) => {
            let fn_name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.to_uppercase())
                .unwrap_or_default();

            let (distinct, args) = match &f.args {
                FunctionArguments::List(FunctionArgumentList { duplicate_treatment, args, .. }) => {
                    let is_distinct = matches!(duplicate_treatment, Some(DuplicateTreatment::Distinct));
                    (is_distinct, args.as_slice())
                }
                _ => (false, &[][..]),
            };

            let col = first_col_from_args(args);

            let func = match fn_name.as_str() {
                "COUNT" if distinct => AggFunc::CountDistinct,
                "COUNT"             => AggFunc::Count,
                "SUM"               => AggFunc::Sum,
                "AVG"               => AggFunc::Avg,
                "MIN"               => AggFunc::Min,
                "MAX"               => AggFunc::Max,
                _                   => return,
            };

            out.push(AlgAggItem {
                alias:    alias.unwrap_or_else(|| fn_name.to_lowercase()),
                func,
                col,
                distinct,
            });
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_agg_from_expr_qe(left,  None, out);
            collect_agg_from_expr_qe(right, None, out);
        }
        Expr::Nested(inner) => collect_agg_from_expr_qe(inner, alias, out),
        _ => {}
    }
}

fn collect_project_items(projection: &[SelectItem]) -> Vec<ProjectItem> {
    projection.iter().filter_map(|item| match item {
        SelectItem::UnnamedExpr(e) => Some(ProjectItem {
            alias: None,
            expr:  sql_expr_to_scalar(e),
        }),
        SelectItem::ExprWithAlias { expr, alias } => Some(ProjectItem {
            alias: Some(alias.value.clone()),
            expr:  sql_expr_to_scalar(expr),
        }),
        SelectItem::Wildcard(_) => Some(ProjectItem {
            alias: None,
            expr:  ScalarExpr::Column("*".into()),
        }),
        _ => None,
    }).collect()
}

// ── AST helpers: aggregation arguments ───────────────────────────────────────

fn first_col_from_args(args: &[FunctionArg]) -> ColumnRef {
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => return ColumnRef::Wildcard,
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                return ColumnRef::Named(id.value.clone());
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                if let Some(last) = parts.last() {
                    return ColumnRef::Named(last.value.clone());
                }
            }
            _ => {}
        }
    }
    ColumnRef::Wildcard
}

// ── AST helpers: GROUP BY ──────────────────────────────────────────────────────

/// Window spec extracted from a TUMBLE() or HOP() call in GROUP BY.
struct SqlWindowSpec {
    size:     Duration,
    slide:    Option<Duration>,
    time_col: Option<String>,
}

fn extract_group_by(group_by: &GroupByExpr) -> Vec<String> {
    let exprs = match group_by {
        GroupByExpr::All(_)         => return vec![],
        GroupByExpr::Expressions(e, _) => e,
    };
    exprs.iter().filter_map(|e| match e {
        Expr::Identifier(id)             => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts)  => parts.last().map(|i| i.value.clone()),
        // Skip TUMBLE/HOP function calls — extracted separately.
        Expr::Function(f) => {
            let name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.to_uppercase())
                .unwrap_or_default();
            if name == "TUMBLE" || name == "HOP" || name == "TIME_BUCKET" {
                None
            } else {
                None // unknown function in GROUP BY — skip
            }
        }
        _                                => None,
    }).collect()
}

/// Extract a TUMBLE / HOP / time_bucket window from the GROUP BY clause.
///
/// Supported forms:
/// - `TUMBLE(ts, INTERVAL '5' MINUTE)` → Tumbling { size: 5m }
/// - `HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)` → Sliding { slide: 1m, size: 5m }
/// - `time_bucket('5 minutes', ts)` → Tumbling { size: 5m }
fn extract_group_by_window(group_by: &GroupByExpr) -> Option<SqlWindowSpec> {
    let exprs = match group_by {
        GroupByExpr::Expressions(e, _) => e,
        _ => return None,
    };
    for expr in exprs {
        if let Expr::Function(f) = expr {
            let name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.to_uppercase())
                .unwrap_or_default();

            let args = match &f.args {
                FunctionArguments::List(FunctionArgumentList { args, .. }) => args,
                _ => continue,
            };

            match name.as_str() {
                "TUMBLE" if args.len() >= 2 => {
                    // TUMBLE(ts_col, interval)
                    let time_col = func_arg_to_col_name(&args[0]);
                    let size = func_arg_to_duration(&args[1])?;
                    return Some(SqlWindowSpec { size, slide: None, time_col });
                }
                "HOP" if args.len() >= 3 => {
                    // HOP(ts_col, slide_interval, size_interval)
                    let time_col = func_arg_to_col_name(&args[0]);
                    let slide = func_arg_to_duration(&args[1])?;
                    let size  = func_arg_to_duration(&args[2])?;
                    return Some(SqlWindowSpec { size, slide: Some(slide), time_col });
                }
                "TIME_BUCKET" if args.len() >= 2 => {
                    // time_bucket('5 minutes', ts_col) — first arg is interval string
                    let size = func_arg_to_duration(&args[0])?;
                    let time_col = func_arg_to_col_name(&args[1]);
                    return Some(SqlWindowSpec { size, slide: None, time_col });
                }
                _ => {}
            }
        }
    }
    None
}

fn func_arg_to_col_name(arg: &FunctionArg) -> Option<String> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) =>
            Some(id.value.clone()),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) =>
            parts.last().map(|i| i.value.clone()),
        _ => None,
    }
}

fn func_arg_to_duration(arg: &FunctionArg) -> Option<Duration> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr_to_duration(expr),
        _ => None,
    }
}

fn expr_to_duration(expr: &Expr) -> Option<Duration> {
    match expr {
        // INTERVAL '5' MINUTE
        Expr::Interval(iv) => {
            let val_str = match iv.value.as_ref() {
                Expr::Value(vws) => match &vws.value {
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
                    Value::Number(n, _) => n.clone(),
                    _ => return None,
                },
                _ => return None,
            };
            let val: u64 = val_str.trim().parse().ok()?;
            let unit = iv.leading_field.as_ref()?;
            let secs = match unit {
                sqlparser::ast::DateTimeField::Second => val,
                sqlparser::ast::DateTimeField::Minute => val * 60,
                sqlparser::ast::DateTimeField::Hour   => val * 3600,
                sqlparser::ast::DateTimeField::Day    => val * 86400,
                _ => return None,
            };
            Some(Duration::from_secs(secs))
        }
        // '5 minutes' string (time_bucket style)
        Expr::Value(vws) => match &vws.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                parse_duration_string(s)
            }
            _ => None,
        },
        _ => None,
    }
}

fn parse_duration_string(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();
    // Try "Nm", "Ns", "Nmin", "N minutes", "N seconds", "N hours"
    let (num_str, unit) = if let Some(n) = s.strip_suffix("minutes") {
        (n.trim(), 60u64)
    } else if let Some(n) = s.strip_suffix("minute") {
        (n.trim(), 60)
    } else if let Some(n) = s.strip_suffix("min") {
        (n.trim(), 60)
    } else if let Some(n) = s.strip_suffix("hours") {
        (n.trim(), 3600)
    } else if let Some(n) = s.strip_suffix("hour") {
        (n.trim(), 3600)
    } else if let Some(n) = s.strip_suffix('h') {
        (n.trim(), 3600)
    } else if let Some(n) = s.strip_suffix("seconds") {
        (n.trim(), 1)
    } else if let Some(n) = s.strip_suffix("second") {
        (n.trim(), 1)
    } else if let Some(n) = s.strip_suffix('s') {
        (n.trim(), 1)
    } else if let Some(n) = s.strip_suffix('m') {
        (n.trim(), 60)
    } else {
        return None;
    };
    let n: u64 = num_str.parse().ok()?;
    Some(Duration::from_secs(n * unit))
}

// ── AST helpers: table name ───────────────────────────────────────────────────

fn extract_table_name(sel: &Select) -> anyhow::Result<String> {
    sel.from.first()
        .and_then(|t| match &t.relation {
            TableFactor::Table { name, .. } => Some(object_name_str(name)),
            _ => None,
        })
        .ok_or_else(|| anyhow!("could not determine table name from FROM clause"))
}

fn object_name_str(name: &ObjectName) -> String {
    name.0.iter()
        .map(|i| i.as_ident().map(|id| id.value.as_str()).unwrap_or(""))
        .collect::<Vec<_>>()
        .join(".")
}

// ── SQL Expr → ScalarExpr ─────────────────────────────────────────────────────

fn sql_expr_to_scalar(expr: &Expr) -> ScalarExpr {
    match expr {
        Expr::Identifier(id) => ScalarExpr::Column(id.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            ScalarExpr::Column(parts.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."))
        }
        Expr::Value(vws) => sql_value_to_scalar(&vws.value),
        Expr::BinaryOp { left, op, right } => {
            let lhs = sql_expr_to_scalar(left);
            let rhs = sql_expr_to_scalar(right);
            let bop = sql_binop_to_algebra(op);
            ScalarExpr::BinaryOp { op: bop, lhs: Box::new(lhs), rhs: Box::new(rhs) }
        }
        Expr::IsNull(inner) => ScalarExpr::IsNull {
            expr:    Box::new(sql_expr_to_scalar(inner)),
            negated: false,
        },
        Expr::IsNotNull(inner) => ScalarExpr::IsNull {
            expr:    Box::new(sql_expr_to_scalar(inner)),
            negated: true,
        },
        Expr::Between { expr, negated, low, high } => ScalarExpr::Between {
            expr:    Box::new(sql_expr_to_scalar(expr)),
            low:     Box::new(sql_expr_to_scalar(low)),
            high:    Box::new(sql_expr_to_scalar(high)),
            negated: *negated,
        },
        Expr::InList { expr, list, negated } => ScalarExpr::InList {
            expr:    Box::new(sql_expr_to_scalar(expr)),
            list:    list.iter().map(sql_expr_to_scalar).collect(),
            negated: *negated,
        },
        Expr::Like { expr, pattern, negated, .. } => {
            let op = if *negated { BinaryOpKind::NotLike } else { BinaryOpKind::Like };
            ScalarExpr::BinaryOp {
                op,
                lhs: Box::new(sql_expr_to_scalar(expr)),
                rhs: Box::new(sql_expr_to_scalar(pattern)),
            }
        }
        Expr::Nested(inner) => sql_expr_to_scalar(inner),
        Expr::Function(f) => {
            let name = f.name.0.last()
                .and_then(|i| i.as_ident())
                .map(|id| id.value.clone())
                .unwrap_or_default();
            ScalarExpr::FunctionCall { name, args: vec![] }
        }
        _ => ScalarExpr::Column("?".into()), // unknown expr → opaque column ref
    }
}

fn sql_value_to_scalar(v: &Value) -> ScalarExpr {
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) =>
            ScalarExpr::Literal(LiteralValue::Str(s.clone())),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                ScalarExpr::Literal(LiteralValue::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                ScalarExpr::Literal(LiteralValue::Float(f))
            } else {
                ScalarExpr::Literal(LiteralValue::Null)
            }
        }
        Value::Boolean(b) => ScalarExpr::Literal(LiteralValue::Bool(*b)),
        Value::Null        => ScalarExpr::Literal(LiteralValue::Null),
        _                  => ScalarExpr::Literal(LiteralValue::Null),
    }
}

fn sql_binop_to_algebra(op: &BinaryOperator) -> BinaryOpKind {
    match op {
        BinaryOperator::Plus      => BinaryOpKind::Add,
        BinaryOperator::Minus     => BinaryOpKind::Sub,
        BinaryOperator::Multiply  => BinaryOpKind::Mul,
        BinaryOperator::Divide    => BinaryOpKind::Div,
        BinaryOperator::Modulo    => BinaryOpKind::Mod,
        BinaryOperator::Eq        => BinaryOpKind::Eq,
        BinaryOperator::NotEq     => BinaryOpKind::Ne,
        BinaryOperator::Lt        => BinaryOpKind::Lt,
        BinaryOperator::LtEq      => BinaryOpKind::Le,
        BinaryOperator::Gt        => BinaryOpKind::Gt,
        BinaryOperator::GtEq      => BinaryOpKind::Ge,
        BinaryOperator::And       => BinaryOpKind::And,
        BinaryOperator::Or        => BinaryOpKind::Or,
        BinaryOperator::BitwiseAnd => BinaryOpKind::BitAnd,
        BinaryOperator::BitwiseOr  => BinaryOpKind::BitOr,
        BinaryOperator::BitwiseXor => BinaryOpKind::BitXor,
        BinaryOperator::StringConcat => BinaryOpKind::Concat,
        _                          => BinaryOpKind::Eq, // unknown → eq
    }
}

// ── JOIN → QueryExpr::Join ────────────────────────────────────────────────────

fn extract_join_qe(sel: &Select) -> Option<(String, JoinKind, Option<ScalarExpr>)> {
    let table_with_joins = sel.from.first()?;
    let join = table_with_joins.joins.first()?;
    let inner_table = match &join.relation {
        TableFactor::Table { name, .. } => object_name_str(name),
        _ => return None,
    };
    let (kind, pred) = match &join.join_operator {
        JoinOperator::Inner(c) =>
            (JoinKind::Inner, join_constraint_to_scalar(c)),
        JoinOperator::LeftOuter(c) =>
            (JoinKind::LeftOuter, join_constraint_to_scalar(c)),
        JoinOperator::RightOuter(c) =>
            (JoinKind::RightOuter, join_constraint_to_scalar(c)),
        JoinOperator::FullOuter(c) =>
            (JoinKind::FullOuter, join_constraint_to_scalar(c)),
        JoinOperator::CrossJoin(_) =>
            (JoinKind::Cross, None),
        _ => return None,
    };
    Some((inner_table, kind, pred))
}

fn join_constraint_to_scalar(c: &JoinConstraint) -> Option<ScalarExpr> {
    match c {
        JoinConstraint::On(e) => Some(sql_expr_to_scalar(e)),
        _ => None,
    }
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse::<u64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn expr_to_col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id)            => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _ => None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::parse_sql_expr;
    use crate::intent_algebra::legacy_expr::QueryExpr;
    use crate::types::AggType;

    fn parse(sql: &str) -> QueryExpr {
        parse_sql_expr(sql).unwrap_or_else(|e| panic!("parse_sql_expr failed: {e}\nSQL: {sql}"))
    }

    fn pq(sql: &str) -> super::super::ParsedQuery {
        super::super::parse_query(sql)
            .unwrap_or_else(|e| panic!("parse_query failed: {e}\nSQL: {sql}"))
    }

    // ── Basic aggregations ────────────────────────────────────────────────────

    #[test]
    fn count_star_no_group_is_exact() {
        let pq = pq("SELECT COUNT(*) FROM hits");
        assert!(pq.exact_required);
        assert!(pq.aggregations.is_empty());
    }

    #[test]
    fn count_star_group_by_is_frequency() {
        let pq = pq("SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID");
        assert!(pq.aggregations.contains(&AggType::Frequency));
        assert!(pq.group_by_labels.contains(&"AdvEngineID".to_string()));
    }

    #[test]
    fn count_distinct_is_cardinality() {
        let pq = pq("SELECT COUNT(DISTINCT UserID) FROM hits");
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(!pq.exact_required);
    }

    #[test]
    fn count_star_order_by_desc_limit_is_topk() {
        let pq = pq(
            "SELECT SearchPhrase, COUNT(*) AS c FROM hits \
             WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    #[test]
    fn avg_with_group_by_is_quantile_p50() {
        let pq = pq("SELECT symbol, AVG(last) FROM hits GROUP BY symbol");
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.quantiles.contains(&0.5));
    }

    #[test]
    fn min_max_with_group_by_are_extremes() {
        let pq = pq("SELECT symbol, MIN(last), MAX(last) FROM hits GROUP BY symbol");
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.quantiles.contains(&0.0));
        assert!(pq.quantiles.contains(&1.0));
    }

    #[test]
    fn min_max_no_group_by_is_exact_minmax() {
        let pq = pq("SELECT MIN(EventDate), MAX(EventDate) FROM hits");
        // MIN/MAX map to Quantile in legacy AggType
        assert!(pq.aggregations.contains(&AggType::Quantile));
    }

    #[test]
    fn sum_is_always_exact() {
        let pq = pq("SELECT SUM(AdvEngineID) FROM hits");
        assert!(pq.exact_required);
    }

    // ── WHERE predicates ──────────────────────────────────────────────────────

    #[test]
    fn where_equality_captured() {
        let pq = pq("SELECT COUNT(*) FROM hits WHERE sectype = 'E' GROUP BY symbol");
        assert_eq!(pq.label_filters.get("sectype").map(String::as_str), Some("E"));
    }

    #[test]
    fn where_inequality_captured() {
        let pq = pq("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID");
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    // ── Multi-aggregation ─────────────────────────────────────────────────────

    #[test]
    fn multi_agg_collects_all() {
        let pq = pq(
            "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) \
             FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality), "missing cardinality");
        assert!(pq.aggregations.contains(&AggType::Frequency),   "missing frequency");
        assert!(pq.aggregations.contains(&AggType::Quantile),    "missing quantile");
        // SUM adds exact_required alongside sketch ops
        assert!(pq.exact_required, "SUM should set exact_required");
    }

    // ── Table / metric name ───────────────────────────────────────────────────

    #[test]
    fn dotted_table_name() {
        let pq = pq("SELECT COUNT(*) FROM financial.last_trade_price GROUP BY symbol");
        assert_eq!(pq.metric_name, "financial.last_trade_price");
    }

    // ── DEBS SQL variants ─────────────────────────────────────────────────────

    #[test]
    fn debs_q6_cardinality() {
        use super::super::QueryHint;
        let pq = pq("SELECT COUNT(DISTINCT symbol) FROM financial.last_trade_price");
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(matches!(pq.hint, Some(QueryHint::DebsCardinality)));
    }

    #[test]
    fn debs_q3_topk() {
        let pq = pq(
            "SELECT symbol, COUNT(*) AS c FROM financial.last_trade_price \
             GROUP BY symbol ORDER BY c DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    // ── COUNT(DISTINCT) with GROUP BY ────────────────────────────────────────

    #[test]
    fn count_distinct_with_group_by() {
        let pq = pq(
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.group_by_labels.contains(&"RegionID".to_string()));
    }

    // ── TUMBLE / HOP windows ────────────────────────────────────────────────

    /// Helper that runs the full pipeline (parse + lower), not just Layer 2.
    fn parse_full(sql: &str) -> QueryExpr {
        super::super::parse_query_expr(sql)
            .unwrap_or_else(|e| panic!("parse_query_expr failed: {e}\nSQL: {sql}"))
    }

    fn has_windowed_agg(e: &QueryExpr) -> bool {
        match e {
            QueryExpr::WindowedAgg { .. } => true,
            QueryExpr::Partition { input, .. }
            | QueryExpr::TopK { input, .. }
            | QueryExpr::Sort { input, .. }
            | QueryExpr::Limit { input, .. } => has_windowed_agg(input),
            _ => false,
        }
    }

    #[test]
    fn tumble_in_group_by_produces_windowed_agg() {
        let expr = parse_full(
            "SELECT symbol, AVG(price) FROM trades \
             GROUP BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)",
        );
        assert!(has_windowed_agg(&expr), "expected WindowedAgg in tree, got {expr:?}");
    }

    #[test]
    fn hop_in_group_by_produces_windowed_agg() {
        let expr = parse_full(
            "SELECT symbol, COUNT(*) FROM trades \
             GROUP BY symbol, HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)",
        );
        assert!(has_windowed_agg(&expr), "expected WindowedAgg in tree, got {expr:?}");
    }

    #[test]
    fn time_bucket_in_group_by_produces_windowed_agg() {
        let expr = parse_full(
            "SELECT symbol, AVG(price) FROM trades \
             GROUP BY symbol, time_bucket('5 minutes', ts)",
        );
        assert!(has_windowed_agg(&expr), "expected WindowedAgg in tree, got {expr:?}");
    }

    #[test]
    fn tumble_layer2_emits_window_node() {
        // Layer 2 only (no lowering): should be Aggregate { input: Window { Source } }
        let expr = parse(
            "SELECT symbol, AVG(price) FROM trades \
             GROUP BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)",
        );
        match &expr {
            QueryExpr::Aggregate { input, .. } => {
                assert!(matches!(input.as_ref(), QueryExpr::Window { .. }),
                    "expected Window inside Aggregate, got {input:?}");
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    // ── UNION ALL → SetOp ─────────────────────────────────────────────────────

    #[test]
    fn union_all_produces_set_op() {
        let expr = parse(
            "SELECT COUNT(DISTINCT UserID) FROM R \
             UNION ALL \
             SELECT COUNT(DISTINCT UserID) FROM S",
        );
        assert!(matches!(expr, QueryExpr::SetOp { .. }));
    }

    // ── Complex queries ───────────────────────────────────────────────────────

    #[test]
    fn complex_multi_agg_multi_dim_group_by_topk() {
        let pq = pq(
            "SELECT region, dc, COUNT(*) AS c, COUNT(DISTINCT UserID), AVG(ResponseTime) \
             FROM hits WHERE env = 'prod' GROUP BY region, dc ORDER BY c DESC LIMIT 5",
        );
        assert!(pq.aggregations.contains(&AggType::Frequency));
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert!(pq.group_by_labels.contains(&"region".to_string()));
        assert!(pq.group_by_labels.contains(&"dc".to_string()));
        assert_eq!(
            pq.label_filters.get("env").map(String::as_str),
            Some("prod")
        );
        assert!(pq.quantiles.contains(&0.5));
    }

    #[test]
    fn complex_union_all_hll_with_where_on_each_branch() {
        let pq = pq(
            "SELECT region, COUNT(DISTINCT UserID) FROM sessions WHERE status = 'active' GROUP BY region \
             UNION ALL \
             SELECT region, COUNT(DISTINCT UserID) FROM sessions WHERE status = 'expired' GROUP BY region",
        );
        assert!(pq.aggregations.contains(&AggType::Cardinality));
        assert!(pq.group_by_labels.contains(&"region".to_string()));
    }
}
