//! Cold-tier fallback adapter (§5.2 of the sketch-DB design).
//!
//! When a query hits a capability-miss — most notably a
//! `TimelineCoverage::Purged` segment whose sketch was aged out —
//! the server falls through to a [`FallbackClient`] impl. This
//! module provides [`ColdFallback`], which answers the query from
//! raw observability samples in a [`ColdStore`] (local FS today,
//! S3 tomorrow — identical key layout, see
//! [`super::cold_store::format`]).
//!
//! Supported query shapes (v1 paper scope):
//!
//! * bare instant vector selector: `metric_name{label="val",...}`
//!   at time `t` — per-series latest value within `[t - 5m, t]`
//!   (the Prometheus default lookback delta)
//! * scalar aggregation without grouping:
//!   `sum|count|avg|min|max ( metric_name{...} )` over the same
//!   instant vector
//!
//! Anything outside that surface delegates to the optional
//! `inner` fallback (typically a Prometheus proxy) — the cold
//! adapter chains with the existing §5.2 forwarding adapter
//! rather than replacing it.
//!
//! Accuracy: results computed here are exact on the set of
//! samples in the cold tier. The adapter does not attempt to
//! reconcile against missing/late samples — the raw tier is the
//! canonical source of truth per the paper architecture.

use async_trait::async_trait;
use axum::http::StatusCode;
use promql_parser::label::MatchOp;
use promql_parser::parser::{Expr, VectorSelector};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::drivers::query::adapters::{ParsedQueryRequest, PrometheusResponse};

use super::cold_store::{ColdStore, RawSample};
use super::metrics::{BYTES_SERVED_COLD_TOTAL, QUERIES_COLD_TOTAL};
use super::{FallbackClient, FallbackResponse};

/// Prometheus's default instant-query lookback delta (5 minutes).
/// Controls how far back the adapter scans the cold store to find
/// the most-recent sample per series.
const INSTANT_LOOKBACK_MS: i64 = 5 * 60 * 1_000;

/// Cold-tier fallback client. Parametrised by the `ColdStore`
/// impl so the same adapter runs over `LocalFsColdStore` in tests
/// and over a (future) S3-backed store in production.
pub struct ColdFallback<S: ColdStore + 'static> {
    store: Arc<S>,
    /// Chain-of-responsibility: unsupported query shapes fall
    /// through to this inner client. Typically a
    /// [`PrometheusHttpFallback`](super::PrometheusHttpFallback)
    /// pointing at the live Prometheus so development / demo
    /// environments stay operational.
    inner: Option<Arc<dyn FallbackClient>>,
}

impl<S: ColdStore + 'static> ColdFallback<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store, inner: None }
    }

    pub fn with_inner(mut self, inner: Arc<dyn FallbackClient>) -> Self {
        self.inner = Some(inner);
        self
    }
}

#[async_trait]
impl<S: ColdStore + 'static> FallbackClient for ColdFallback<S> {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        let ast = match promql_parser::parser::parse(&request.query) {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    "cold fallback: PromQL parse failed ({}); delegating to inner",
                    e
                );
                return self.delegate(request).await;
            }
        };

        let plan = match plan_query(&ast) {
            Some(p) => p,
            None => {
                debug!(
                    "cold fallback: unsupported query shape for '{}'; delegating to inner",
                    request.query
                );
                return self.delegate(request).await;
            }
        };

        let query_time_ms = (request.time * 1_000.0) as i64;
        let start_ms = query_time_ms - INSTANT_LOOKBACK_MS;
        let end_ms = query_time_ms + 1;

        let samples = match self.store.scan(&plan.metric, start_ms, end_ms).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "cold fallback: scan failed for metric '{}': {}",
                    plan.metric, e
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };

        // Telemetry — size is a stable proxy for "cold work" even
        // when the post-filter result has zero samples.
        let bytes = approx_wire_bytes(&samples);
        let shape = plan.op.as_label();
        BYTES_SERVED_COLD_TOTAL
            .with_label_values(&[plan.metric.as_str(), shape])
            .inc_by(bytes as f64);
        QUERIES_COLD_TOTAL
            .with_label_values(&[plan.metric.as_str(), shape])
            .inc();

        let filtered = samples
            .into_iter()
            .filter(|s| plan.matches_labels(&s.labels))
            .collect::<Vec<_>>();

        let latest = latest_per_series(&filtered);
        let data = compute_result(&plan, &latest, &request.query, query_time_ms);
        let resp = PrometheusResponse::success(data);
        let value = serde_json::to_value(resp).unwrap_or_else(|_| json!({"status":"error"}));
        Ok(FallbackResponse::Json(value))
    }

    async fn execute_query_with_headers(
        &self,
        request: &ParsedQueryRequest,
        _headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        self.execute_query(request).await
    }

    async fn get_runtime_info(&self) -> Result<Value, StatusCode> {
        // Runtime info is meaningless for a cold store; defer to
        // inner if configured, else return empty.
        match &self.inner {
            Some(inner) => inner.get_runtime_info().await,
            None => Ok(json!({})),
        }
    }
}

impl<S: ColdStore + 'static> ColdFallback<S> {
    async fn delegate(&self, request: &ParsedQueryRequest) -> Result<FallbackResponse, StatusCode> {
        match &self.inner {
            Some(inner) => inner.execute_query(request).await,
            None => {
                // No chain — return an empty Prometheus success
                // response rather than a 5xx; the adapter is
                // advisory for query shapes it can't handle.
                let resp = PrometheusResponse::success(json!({
                    "resultType": "vector",
                    "result": []
                }));
                Ok(FallbackResponse::Json(
                    serde_json::to_value(resp).unwrap_or(json!({"status":"error"})),
                ))
            }
        }
    }
}

/// The query op we recognise for cold evaluation. Kept narrow so
/// the adapter's behaviour is obvious from the outside — anything
/// else delegates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryOp {
    /// Bare instant vector (`metric{labels}`).
    Selector,
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

impl QueryOp {
    fn as_label(&self) -> &'static str {
        match self {
            QueryOp::Selector => "selector",
            QueryOp::Sum => "sum",
            QueryOp::Count => "count",
            QueryOp::Avg => "avg",
            QueryOp::Min => "min",
            QueryOp::Max => "max",
        }
    }

    fn from_agg_str(s: &str) -> Option<QueryOp> {
        match s.to_ascii_lowercase().as_str() {
            "sum" => Some(QueryOp::Sum),
            "count" => Some(QueryOp::Count),
            "avg" => Some(QueryOp::Avg),
            "min" => Some(QueryOp::Min),
            "max" => Some(QueryOp::Max),
            _ => None,
        }
    }
}

/// Matcher set we understand. Regex matchers (`=~`, `!~`) are
/// deliberately out of scope for v1 — passing a regex matcher
/// causes the adapter to delegate upstream.
#[derive(Debug, Clone)]
struct LabelPredicate {
    name: String,
    value: String,
    equals: bool,
}

struct QueryPlan {
    metric: String,
    predicates: Vec<LabelPredicate>,
    op: QueryOp,
}

impl QueryPlan {
    fn matches_labels(&self, labels: &BTreeMap<String, String>) -> bool {
        for p in &self.predicates {
            let hit = labels.get(&p.name).map(|v| v == &p.value).unwrap_or(false);
            if p.equals && !hit {
                return false;
            }
            if !p.equals && hit {
                return false;
            }
        }
        true
    }
}

/// Inspect the PromQL AST and, if it's a shape we support, return
/// the extracted `(metric, predicates, op)`. Returns `None`
/// otherwise — caller delegates to inner fallback.
fn plan_query(ast: &Expr) -> Option<QueryPlan> {
    match ast {
        Expr::VectorSelector(vs) => {
            let (metric, predicates) = selector_to_plan(vs)?;
            Some(QueryPlan {
                metric,
                predicates,
                op: QueryOp::Selector,
            })
        }
        Expr::Paren(p) => plan_query(&p.expr),
        Expr::Aggregate(agg) => {
            // Only recognise no-grouping-modifier, no-param aggs —
            // `sum by (...)` / `topk(k, expr)` exceed v1 scope.
            if agg.modifier.is_some() {
                return None;
            }
            if agg.param.is_some() {
                return None;
            }
            let op = QueryOp::from_agg_str(&agg.op.to_string())?;
            let vs = match agg.expr.as_ref() {
                Expr::VectorSelector(vs) => vs,
                Expr::Paren(p) => match p.expr.as_ref() {
                    Expr::VectorSelector(vs) => vs,
                    _ => return None,
                },
                _ => return None,
            };
            let (metric, predicates) = selector_to_plan(vs)?;
            Some(QueryPlan {
                metric,
                predicates,
                op,
            })
        }
        _ => None,
    }
}

fn selector_to_plan(vs: &VectorSelector) -> Option<(String, Vec<LabelPredicate>)> {
    let metric = vs.name.clone()?;
    let mut predicates = Vec::new();
    for m in &vs.matchers.matchers {
        // __name__ is already captured via vs.name — skip any
        // explicit __name__ matcher that duplicates it.
        if m.name == "__name__" {
            continue;
        }
        let equals = match m.op {
            MatchOp::Equal => true,
            MatchOp::NotEqual => false,
            _ => return None, // regex matchers out of scope
        };
        predicates.push(LabelPredicate {
            name: m.name.clone(),
            value: m.value.clone(),
            equals,
        });
    }
    Some((metric, predicates))
}

/// Reduce a pool of samples to one-per-unique-label-set, taking
/// the sample with the largest `ts_ms`. Mirrors Prometheus's
/// instant-vector semantics.
fn latest_per_series(samples: &[RawSample]) -> Vec<RawSample> {
    let mut seen: HashMap<Vec<(String, String)>, RawSample> = HashMap::new();
    for s in samples {
        let key: Vec<_> = s
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        seen.entry(key)
            .and_modify(|existing| {
                if s.ts_ms > existing.ts_ms {
                    *existing = s.clone();
                }
            })
            .or_insert_with(|| s.clone());
    }
    seen.into_values().collect()
}

/// Build the Prometheus HTTP `data` payload for the query result.
/// * `Selector` → vector with one entry per series.
/// * `sum|count|avg|min|max` → single-element vector with no
///   `__name__` label (matches `sum(foo)` Prometheus output).
fn compute_result(
    plan: &QueryPlan,
    latest: &[RawSample],
    _query: &str,
    query_time_ms: i64,
) -> Value {
    let ts_s = (query_time_ms as f64) / 1_000.0;
    match plan.op {
        QueryOp::Selector => {
            let items = latest
                .iter()
                .map(|s| {
                    let mut m = serde_json::Map::new();
                    m.insert("__name__".to_string(), Value::String(plan.metric.clone()));
                    for (k, v) in &s.labels {
                        m.insert(k.clone(), Value::String(v.clone()));
                    }
                    json!({
                        "metric": Value::Object(m),
                        "value": [ts_s, format_prom_float(s.value)],
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "resultType": "vector",
                "result": items,
            })
        }
        op => {
            let agg = aggregate(op, latest);
            let value = match agg {
                Some(v) => json!([ts_s, format_prom_float(v)]),
                // Empty-input semantics: Prometheus returns empty
                // result for sum/min/max/avg over no samples.
                None => {
                    return json!({
                        "resultType": "vector",
                        "result": [],
                    })
                }
            };
            json!({
                "resultType": "vector",
                "result": [
                    {
                        "metric": {},
                        "value": value,
                    }
                ],
            })
        }
    }
}

fn aggregate(op: QueryOp, latest: &[RawSample]) -> Option<f64> {
    if latest.is_empty() {
        // count over empty = 0 is what Prometheus does; other ops
        // return empty vector.
        return match op {
            QueryOp::Count => Some(0.0),
            _ => None,
        };
    }
    let xs = latest.iter().map(|s| s.value);
    Some(match op {
        QueryOp::Sum => xs.sum(),
        QueryOp::Count => latest.len() as f64,
        QueryOp::Avg => latest.iter().map(|s| s.value).sum::<f64>() / (latest.len() as f64),
        QueryOp::Min => xs.fold(f64::INFINITY, f64::min),
        QueryOp::Max => xs.fold(f64::NEG_INFINITY, f64::max),
        QueryOp::Selector => unreachable!("Selector handled in compute_result"),
    })
}

/// Stable, Prometheus-style float formatting for the HTTP JSON
/// `value` field — integers render without a trailing `.0`.
fn format_prom_float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        };
    }
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn approx_wire_bytes(samples: &[RawSample]) -> usize {
    // Rough proxy: one JSON line per sample. Avoids re-serialising
    // every scan into memory just to count. Good enough for
    // observability dashboards.
    samples
        .iter()
        .map(|s| {
            let label_bytes: usize = s
                .labels
                .iter()
                .map(|(k, v)| k.len() + v.len() + 6) // "k":"v",
                .sum();
            // {"ts_ms":<13>,"labels":{...},"value":<~20>} + newline
            13 + 12 + label_bytes + 12 + 20 + 2
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rs(ts_ms: i64, labels: &[(&str, &str)], value: f64) -> RawSample {
        RawSample {
            ts_ms,
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            value,
        }
    }

    #[test]
    fn plan_bare_selector() {
        let ast = promql_parser::parser::parse("up{zone=\"a\"}").unwrap();
        let plan = plan_query(&ast).unwrap();
        assert_eq!(plan.metric, "up");
        assert_eq!(plan.predicates.len(), 1);
        assert_eq!(plan.op, QueryOp::Selector);
    }

    #[test]
    fn plan_sum_over_selector() {
        let ast = promql_parser::parser::parse("sum(up)").unwrap();
        let plan = plan_query(&ast).unwrap();
        assert_eq!(plan.op, QueryOp::Sum);
    }

    #[test]
    fn plan_sum_by_is_unsupported() {
        let ast = promql_parser::parser::parse("sum by (zone) (up)").unwrap();
        assert!(plan_query(&ast).is_none());
    }

    #[test]
    fn plan_rate_is_unsupported() {
        let ast = promql_parser::parser::parse("rate(up[1m])").unwrap();
        assert!(plan_query(&ast).is_none());
    }

    #[test]
    fn plan_regex_matcher_is_unsupported() {
        let ast = promql_parser::parser::parse("up{zone=~\"a.*\"}").unwrap();
        assert!(plan_query(&ast).is_none());
    }

    #[test]
    fn latest_per_series_picks_newest() {
        let ss = vec![
            rs(100, &[("zone", "a")], 1.0),
            rs(200, &[("zone", "a")], 2.0),
            rs(150, &[("zone", "b")], 9.0),
        ];
        let got = latest_per_series(&ss);
        assert_eq!(got.len(), 2);
        let a = got.iter().find(|s| s.labels["zone"] == "a").unwrap();
        let b = got.iter().find(|s| s.labels["zone"] == "b").unwrap();
        assert_eq!(a.value, 2.0);
        assert_eq!(b.value, 9.0);
    }

    #[test]
    fn aggregate_ops() {
        let samples = vec![
            rs(1, &[("z", "a")], 1.0),
            rs(1, &[("z", "b")], 2.0),
            rs(1, &[("z", "c")], 3.0),
        ];
        assert_eq!(aggregate(QueryOp::Sum, &samples), Some(6.0));
        assert_eq!(aggregate(QueryOp::Count, &samples), Some(3.0));
        assert_eq!(aggregate(QueryOp::Avg, &samples), Some(2.0));
        assert_eq!(aggregate(QueryOp::Min, &samples), Some(1.0));
        assert_eq!(aggregate(QueryOp::Max, &samples), Some(3.0));
    }

    #[test]
    fn aggregate_empty_count_is_zero() {
        assert_eq!(aggregate(QueryOp::Count, &[]), Some(0.0));
        assert_eq!(aggregate(QueryOp::Sum, &[]), None);
    }

    #[test]
    fn format_prom_float_matches_prometheus_conventions() {
        assert_eq!(format_prom_float(1.0), "1");
        assert_eq!(format_prom_float(1.5), "1.5");
        assert_eq!(format_prom_float(f64::INFINITY), "+Inf");
        assert_eq!(format_prom_float(f64::NAN), "NaN");
    }

    #[test]
    fn label_predicate_equal_and_not_equal() {
        let plan = QueryPlan {
            metric: "m".into(),
            predicates: vec![
                LabelPredicate {
                    name: "zone".into(),
                    value: "a".into(),
                    equals: true,
                },
                LabelPredicate {
                    name: "env".into(),
                    value: "prod".into(),
                    equals: false,
                },
            ],
            op: QueryOp::Selector,
        };
        let match_a: BTreeMap<String, String> = [("zone", "a"), ("env", "dev")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let no_match: BTreeMap<String, String> = [("zone", "a"), ("env", "prod")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(plan.matches_labels(&match_a));
        assert!(!plan.matches_labels(&no_match));
    }
}
