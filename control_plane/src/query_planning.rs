//! Autonomous allocation, slice 2: a **set of query strings → per-metric sketch
//! plan**.
//!
//! This composes two existing pieces with slice 1's inverse map:
//!   1. [`analyze_promql_for_asap_tier`] lowers each query to
//!      `ASAPTierCandidate`s, each already carrying a `metric_name` and the
//!      `required_capability` the query needs (and an `unsupported` reason for
//!      the parts that only the cold tier can answer).
//!   2. [`required_sketches_for_capabilities`](crate::sketch_selection::required_sketches_for_capabilities)
//!      (slice 1) maps a capability set to the `SketchType` families that satisfy it.
//!
//! The result, [`QuerySetPlan`], is the warm-tier half of autonomous
//! allocation: "given these queries, allocate these sketches per metric", plus
//! the list of queries that fall through to the cold tier. Slice 3 layers the
//! `ε → (p, τ)` knobs onto each metric; slice 4 emits a `StreamingConfig`.

use std::collections::BTreeMap;

use crate::asap_tier_analysis::{analyze_promql_for_asap_tier, UnsupportedReason};
use crate::physical::runtime_capability::Capability;
use crate::sketch_selection::required_sketches_for_capabilities;
use crate::types::SketchType;

/// The sketch allocation derived for one metric across the whole query set.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSketchPlan {
    pub metric_name: String,
    /// The distinct warm-tier capabilities the query set demands of this
    /// metric (order-stable, first-seen wins).
    pub capabilities: Vec<Capability>,
    /// The sketch families to allocate so every demanded capability is
    /// satisfied (de-duplicated union over `capabilities`).
    pub sketches: Vec<SketchType>,
}

/// A query (or query fragment) that the warm sketch tier cannot answer and that
/// must be routed to the cold tier.
#[derive(Debug, Clone, PartialEq)]
pub struct ColdQuery {
    pub query: String,
    /// Why it isn't ASAP-tier-answerable (`None` only in the degenerate case
    /// where the analyzer returned no candidates and no explicit reason).
    pub reason: Option<UnsupportedReason>,
}

/// The full allocation plan for a query set: what to allocate warm, and what
/// falls through to cold.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QuerySetPlan {
    /// Per-metric warm sketch allocation, keyed/ordered by metric name.
    pub per_metric: Vec<MetricSketchPlan>,
    /// Queries (or partially-supported queries) needing the cold tier.
    pub cold_only: Vec<ColdQuery>,
}

impl QuerySetPlan {
    /// All sketch families this plan allocates, across every metric
    /// (de-duplicated, order-stable). Convenience for capacity sizing.
    pub fn all_sketches(&self) -> Vec<SketchType> {
        let mut out: Vec<SketchType> = Vec::new();
        for m in &self.per_metric {
            for s in &m.sketches {
                if !out.contains(s) {
                    out.push(s.clone());
                }
            }
        }
        out
    }
}

/// Plan the warm sketch allocation for a set of queries.
///
/// Each query is lowered via the ASAP-tier analyzer; every answerable candidate
/// contributes its `required_capability` to its metric's bucket, and any
/// unsupported reason (or a fully-unsupported query) is recorded under
/// `cold_only`. Per metric, the accumulated capabilities are unioned into the
/// sketch families to allocate.
///
/// A single query can land in BOTH halves — e.g. `sum(quantile_over_time(...))`
/// yields a warm quantile candidate (→ DDSketch/KLL) while an exact-`sum`
/// fragment routes cold. That partial-support split is intentional.
pub fn plan_sketches_for_queries<I, S>(queries: I) -> QuerySetPlan
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    // BTreeMap → deterministic metric ordering regardless of query order.
    let mut by_metric: BTreeMap<String, Vec<Capability>> = BTreeMap::new();
    let mut cold_only: Vec<ColdQuery> = Vec::new();

    for q in queries {
        let q = q.as_ref();
        let analysis = analyze_promql_for_asap_tier(q);

        for cand in &analysis.candidates {
            let caps = by_metric.entry(cand.metric_name.clone()).or_default();
            if !caps.contains(&cand.required_capability) {
                caps.push(cand.required_capability.clone());
            }
        }

        // No answerable candidate at all, OR a partially-supported query with a
        // residual unsupported reason → the (residual) query goes cold.
        if analysis.candidates.is_empty() || analysis.unsupported.is_some() {
            cold_only.push(ColdQuery {
                query: q.to_string(),
                reason: analysis.unsupported,
            });
        }
    }

    let per_metric = by_metric
        .into_iter()
        .map(|(metric_name, capabilities)| {
            let sketches = required_sketches_for_capabilities(&capabilities);
            MetricSketchPlan {
                metric_name,
                capabilities,
                sketches,
            }
        })
        .collect();

    QuerySetPlan {
        per_metric,
        cold_only,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_query_allocates_a_quantile_sketch() {
        let plan = plan_sketches_for_queries([
            "quantile_over_time(0.99, http_requests_total_latency_ms[5m])",
        ]);
        let m = plan
            .per_metric
            .iter()
            .find(|m| m.metric_name == "http_requests_total_latency_ms")
            .expect("metric present");
        // a quantile demands a quantile-family sketch
        assert!(
            m.sketches.contains(&SketchType::DDSketch) || m.sketches.contains(&SketchType::KLL)
        );
    }

    #[test]
    fn distinct_metrics_get_separate_plans() {
        let plan = plan_sketches_for_queries([
            "quantile_over_time(0.99, latency_ms[5m])",
            "count(http_requests_total)",
        ]);
        // at least the quantile metric must appear with a quantile sketch
        let latency = plan
            .per_metric
            .iter()
            .find(|m| m.metric_name == "latency_ms");
        assert!(latency.is_some(), "latency_ms should be planned: {plan:?}");
    }

    #[test]
    fn unparseable_query_falls_through_to_cold() {
        let plan = plan_sketches_for_queries(["this is not valid promql @@@"]);
        assert!(plan.per_metric.is_empty());
        assert_eq!(plan.cold_only.len(), 1);
    }

    #[test]
    fn same_metric_two_queries_unions_capabilities_without_dup() {
        // two quantile queries on the same metric → one metric plan, the
        // quantile capability not duplicated.
        let plan = plan_sketches_for_queries([
            "quantile_over_time(0.99, m_latency[5m])",
            "quantile_over_time(0.50, m_latency[5m])",
        ]);
        let m = plan
            .per_metric
            .iter()
            .find(|m| m.metric_name == "m_latency")
            .expect("metric present");
        // capabilities are de-duplicated (both queries need the same quantile cap)
        assert_eq!(
            m.capabilities.len(),
            1,
            "caps deduped: {:?}",
            m.capabilities
        );
    }

    #[test]
    fn canonical_e2e_query_suite_produces_a_plan() {
        // The real queries-e2e.json shapes: nested aggregations, topk, count.
        // We don't pin each classification (that's the analyzer's contract) —
        // we assert the planner survives them and produces a sensible plan:
        // the quantile query must yield a quantile-family sketch somewhere.
        let plan = plan_sketches_for_queries([
            "quantile_over_time(0.99, http_requests_total_latency_ms[5m])",
            "max by (zone) (quantile_over_time(0.99, http_requests_total_latency_ms[5m]))",
            "sum by (zone) (http_requests_total)",
            "sum by (zone) (rate(http_requests_total[5m]))",
            "topk(5, sum by (zone) (rate(http_requests_total[5m])))",
            "count(http_requests_total{zone=\"z0\"})",
        ]);
        // every allocated sketch must trace back to a demanded capability
        for m in &plan.per_metric {
            assert!(
                !m.capabilities.is_empty(),
                "metric {} has sketches but no capabilities",
                m.metric_name
            );
        }
        // the quantile query guarantees a quantile-family sketch is planned
        let all = plan.all_sketches();
        assert!(
            all.contains(&SketchType::DDSketch) || all.contains(&SketchType::KLL),
            "quantile query should allocate a quantile sketch; got {all:?} / {plan:?}"
        );
    }

    #[test]
    fn empty_query_set_yields_empty_plan() {
        let plan = plan_sketches_for_queries(Vec::<&str>::new());
        assert!(plan.per_metric.is_empty());
        assert!(plan.cold_only.is_empty());
        assert!(plan.all_sketches().is_empty());
    }
}
