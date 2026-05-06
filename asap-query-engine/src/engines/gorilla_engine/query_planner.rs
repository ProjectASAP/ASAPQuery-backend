//! PromQL → [`QueryPlan`] translator for the Phase-4 Gorilla engine.
//!
//! The Phase-4 surface is intentionally narrow: instant-vector
//! queries that wrap a single matrix selector with one of the
//! supported `*_over_time` / `rate` / `increase` functions, OR
//! a top-level `quantile_over_time(φ, m[range])` /
//! `topk(k, m[range])`-style aggregation.
//!
//! Time range is `(now - lookback_ms, now)` where `now` is the
//! caller-supplied "query time" — fixed to `chrono::Utc::now()`
//! when not specified, so callers that don't care about backdating
//! a query don't need to thread a clock through.

use std::time::SystemTime;

use chrono::Utc;
use promql_parser::parser::{
    AggregateExpr, Call, Expr, FunctionArgs, MatrixSelector, NumberLiteral, ParenExpr,
    VectorSelector,
};

/// Statistic to compute, alongside any extra parameters
/// (quantile φ, top-k k).
#[derive(Debug, Clone, PartialEq)]
pub enum QueryStatistic {
    /// `sum_over_time(m[range])`
    SumOverTime,
    /// `count_over_time(m[range])`
    CountOverTime,
    /// `avg_over_time(m[range])` (= sum / count)
    AvgOverTime,
    /// `min_over_time(m[range])`
    MinOverTime,
    /// `max_over_time(m[range])`
    MaxOverTime,
    /// `rate(m[range])` — `(last - first) / range_seconds`
    Rate,
    /// `increase(m[range])` — `last - first`
    Increase,
    /// `quantile_over_time(φ, m[range])`
    QuantileOverTime { phi: f64 },
    /// `topk(k, sum_over_time(m[range]))`-style aggregation. The
    /// MVP Phase 4 implementation returns the sum of the top-`k`
    /// sample values in the range — once Phase 5 adds spatial
    /// grouping the executor will return a per-group vector.
    TopK { k: usize },
}

impl QueryStatistic {
    /// True iff the executor can answer this statistic via the
    /// streaming-additive path; false → buffered path (everything
    /// has to be in memory before producing the answer).
    pub fn is_streaming_additive(&self) -> bool {
        matches!(
            self,
            Self::SumOverTime
                | Self::CountOverTime
                | Self::AvgOverTime
                | Self::MinOverTime
                | Self::MaxOverTime
                | Self::Rate
                | Self::Increase
        )
    }
}

/// Output of [`plan_query`].
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub metric: String,
    /// Half-open `[start_ms, end_ms)` request window. Computed as
    /// `(now_ms - range_ms, now_ms)` from the matrix selector's
    /// `[range]` duration.
    pub time_range_ms: (i64, i64),
    pub statistic: QueryStatistic,
}

/// Parse `query` and produce a [`QueryPlan`]. `now` defaults to
/// the system clock; the [`plan_query_at`] variant lets tests pin
/// a deterministic timestamp.
pub fn plan_query(query: &str) -> Result<QueryPlan, String> {
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_else(|_| Utc::now().timestamp_millis());
    plan_query_at(query, now_ms)
}

/// As [`plan_query`], with a caller-supplied `now_ms`.
pub fn plan_query_at(query: &str, now_ms: i64) -> Result<QueryPlan, String> {
    let ast = promql_parser::parser::parse(query).map_err(|e| format!("parse: {e}"))?;
    plan_from_ast(&ast, now_ms)
}

fn plan_from_ast(ast: &Expr, now_ms: i64) -> Result<QueryPlan, String> {
    match ast {
        Expr::Paren(ParenExpr { expr }) => plan_from_ast(expr, now_ms),
        Expr::Call(call) => plan_from_call(call, now_ms),
        Expr::Aggregate(agg) => plan_from_aggregate(agg, now_ms),
        other => Err(format!(
            "unsupported top-level expression: {:?}; the Gorilla engine \
             expects a single function call (rate/increase/*_over_time) \
             or topk(k, ...) aggregation",
            std::mem::discriminant(other)
        )),
    }
}

fn plan_from_call(call: &Call, now_ms: i64) -> Result<QueryPlan, String> {
    let name = call.func.name.to_lowercase();
    match name.as_str() {
        "sum_over_time"
        | "count_over_time"
        | "avg_over_time"
        | "min_over_time"
        | "max_over_time"
        | "rate"
        | "increase" => {
            let ms = expect_single_matrix_arg(&call.args, &name)?;
            let (metric, range_ms) = matrix_metric_and_range_ms(ms);
            let stat = match name.as_str() {
                "sum_over_time" => QueryStatistic::SumOverTime,
                "count_over_time" => QueryStatistic::CountOverTime,
                "avg_over_time" => QueryStatistic::AvgOverTime,
                "min_over_time" => QueryStatistic::MinOverTime,
                "max_over_time" => QueryStatistic::MaxOverTime,
                "rate" => QueryStatistic::Rate,
                "increase" => QueryStatistic::Increase,
                _ => unreachable!(),
            };
            Ok(QueryPlan {
                metric,
                time_range_ms: (now_ms - range_ms, now_ms),
                statistic: stat,
            })
        }
        "quantile_over_time" => {
            // quantile_over_time(φ, m[range])
            if call.args.args.len() != 2 {
                return Err(format!(
                    "quantile_over_time expects 2 args, got {}",
                    call.args.args.len()
                ));
            }
            let phi = expect_number(&call.args.args[0], "quantile_over_time φ")?;
            let ms = expect_matrix_selector(&call.args.args[1], "quantile_over_time")?;
            let (metric, range_ms) = matrix_metric_and_range_ms(ms);
            Ok(QueryPlan {
                metric,
                time_range_ms: (now_ms - range_ms, now_ms),
                statistic: QueryStatistic::QuantileOverTime { phi },
            })
        }
        other => Err(format!(
            "unsupported PromQL function: {other}; the Gorilla engine \
             supports rate/increase/*_over_time/quantile_over_time"
        )),
    }
}

fn plan_from_aggregate(agg: &AggregateExpr, now_ms: i64) -> Result<QueryPlan, String> {
    // PromQL grammar requires aggregation operators to take a
    // vector — so the legal Phase-4 spellings are e.g.
    // `topk(2, sum_over_time(m[10s]))`. We strip the outer
    // aggregation, recurse into the inner call to recover the
    // `(metric, range)` pair, then overlay the TopK statistic.
    let op_str = format!("{}", agg.op);
    if !op_str.eq_ignore_ascii_case("topk") {
        return Err(format!(
            "unsupported top-level aggregation: {op_str}; only `topk(k, ...)` \
             is supported in Phase 4"
        ));
    }
    let k_expr = agg
        .param
        .as_deref()
        .ok_or_else(|| "topk requires a numeric parameter (k)".to_string())?;
    let k = expect_number(k_expr, "topk k")?;
    if !k.is_finite() || k <= 0.0 {
        return Err(format!("topk k must be positive, got {k}"));
    }
    // Recurse into the inner expression — it can be a matrix
    // selector (handled by [`matrix_metric_and_range_ms`]
    // directly) OR a vector-returning function call (the legal
    // PromQL spelling). Either way we end up with a
    // `(metric, range_ms)` pair we can overlay TopK on.
    let inner_plan = match &*agg.expr {
        Expr::MatrixSelector(ms) => {
            let (metric, range_ms) = matrix_metric_and_range_ms(ms);
            QueryPlan {
                metric,
                time_range_ms: (now_ms - range_ms, now_ms),
                statistic: QueryStatistic::SumOverTime, // overlay below
            }
        }
        _ => plan_from_ast(&agg.expr, now_ms)?,
    };
    Ok(QueryPlan {
        metric: inner_plan.metric,
        time_range_ms: inner_plan.time_range_ms,
        statistic: QueryStatistic::TopK { k: k as usize },
    })
}

fn expect_single_matrix_arg<'a>(
    args: &'a FunctionArgs,
    fname: &str,
) -> Result<&'a MatrixSelector, String> {
    if args.args.len() != 1 {
        return Err(format!(
            "{fname} expects 1 matrix-selector arg, got {}",
            args.args.len()
        ));
    }
    expect_matrix_selector(&args.args[0], fname)
}

fn expect_matrix_selector<'a>(expr: &'a Expr, ctx: &str) -> Result<&'a MatrixSelector, String> {
    match expr {
        Expr::MatrixSelector(ms) => Ok(ms),
        Expr::Paren(ParenExpr { expr }) => expect_matrix_selector(expr, ctx),
        other => Err(format!(
            "{ctx}: expected matrix selector `metric[range]`, got {:?}",
            std::mem::discriminant(other)
        )),
    }
}

fn expect_number(expr: &Expr, ctx: &str) -> Result<f64, String> {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val }) => Ok(*val),
        Expr::Paren(ParenExpr { expr }) => expect_number(expr, ctx),
        other => Err(format!(
            "{ctx}: expected numeric literal, got {:?}",
            std::mem::discriminant(other)
        )),
    }
}

fn matrix_metric_and_range_ms(ms: &MatrixSelector) -> (String, i64) {
    let metric = vector_selector_metric(&ms.vs);
    let range_ms = ms.range.as_millis() as i64;
    (metric, range_ms)
}

fn vector_selector_metric(vs: &VectorSelector) -> String {
    if let Some(name) = &vs.name {
        return name.clone();
    }
    // Fallback: inspect matchers for an `__name__` exact match.
    for m in vs.matchers.matchers.iter() {
        if m.name == "__name__" {
            return m.value.clone();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_715_000_000_000;

    #[test]
    fn plans_sum_over_time() {
        let plan = plan_query_at("sum_over_time(http_requests_total[5m])", NOW).unwrap();
        assert_eq!(plan.metric, "http_requests_total");
        assert_eq!(plan.statistic, QueryStatistic::SumOverTime);
        assert_eq!(plan.time_range_ms, (NOW - 5 * 60_000, NOW));
    }

    #[test]
    fn plans_quantile_over_time() {
        let plan = plan_query_at("quantile_over_time(0.99, latency_ms[1m])", NOW).unwrap();
        assert_eq!(plan.metric, "latency_ms");
        assert!(matches!(
            plan.statistic,
            QueryStatistic::QuantileOverTime { phi } if (phi - 0.99).abs() < 1e-12
        ));
    }

    #[test]
    fn plans_topk() {
        // Legal PromQL spelling: aggregation wraps a vector-returning
        // function call. The Phase-4 planner peels off the outer
        // `topk` and recovers the `(metric, range)` pair from the
        // inner `sum_over_time(...)`.
        let plan = plan_query_at("topk(3, sum_over_time(m[10s]))", NOW).unwrap();
        assert!(matches!(plan.statistic, QueryStatistic::TopK { k } if k == 3));
        assert_eq!(plan.metric, "m");
        assert_eq!(plan.time_range_ms, (NOW - 10_000, NOW));
    }

    #[test]
    fn rejects_binary_expression() {
        assert!(plan_query_at("foo + bar", NOW).is_err());
    }

    #[test]
    fn streaming_classification() {
        assert!(QueryStatistic::SumOverTime.is_streaming_additive());
        assert!(QueryStatistic::Rate.is_streaming_additive());
        assert!(!QueryStatistic::QuantileOverTime { phi: 0.5 }.is_streaming_additive());
        assert!(!QueryStatistic::TopK { k: 1 }.is_streaming_additive());
    }
}
