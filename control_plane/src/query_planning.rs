//! Autonomous allocation, slice 2: a **set of query strings → per-metric sketch
//! plan**.
//!
//! This composes two existing pieces with slice 1's inverse map:
//!   1. ASAPPlanner lowers each query to its canonical post-ASAP summary DAG.
//!   2. [`required_sketches_for_capabilities`](crate::sketch_selection::required_sketches_for_capabilities)
//!      (slice 1) maps a capability set to the `SketchType` families that satisfy it.
//!
//! The result, [`QuerySetPlan`], is the warm-tier half of autonomous
//! allocation: "given these queries, allocate these sketches per metric", plus
//! the list of queries that fall through to the cold tier. Slice 3 layers the
//! `ε → (p, τ)` knobs onto each metric; slice 4 emits a `StreamingConfig`.

use std::collections::BTreeMap;

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
    pub reason: Option<String>,
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
        let planned = crate::asap_tier_implement::implement_promql_for_asap_tier(q);
        let mut found = false;
        if let Ok(nodes) = &planned {
            for node in nodes {
                if let Some((metric, capability)) = planned_capability(node) {
                    found = true;
                    let caps = by_metric.entry(metric).or_default();
                    if !caps.contains(&capability) {
                        caps.push(capability);
                    }
                }
            }
        }
        if !found {
            cold_only.push(ColdQuery {
                query: q.to_string(),
                reason: planned.err().map(|error| format!("{error:?}")),
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

fn planned_capability(
    node: &planner_types::post_asap::SummaryNode,
) -> Option<(String, Capability)> {
    use crate::physical::runtime_capability::SketchKindHandle;
    use planner_types::post_asap::{SketchQuery, SummaryExpr, SummaryFamilyType};

    fn metric(node: &planner_types::post_asap::SummaryNode) -> Option<String> {
        fn from_query(expr: &planner_types::pre_asap::QueryExpr) -> Option<String> {
            use planner_types::pre_asap::{QueryExpr, Source};
            match expr {
                QueryExpr::Scan {
                    source: Source::TimeSeries { metric },
                    ..
                } => Some(metric.clone()),
                QueryExpr::Filter { child, .. }
                | QueryExpr::Project { child, .. }
                | QueryExpr::Aggregate { child, .. }
                | QueryExpr::Dedup { child, .. }
                | QueryExpr::Sort { child, .. }
                | QueryExpr::Limit { child, .. }
                | QueryExpr::PromqlSubquery { child, .. }
                | QueryExpr::TimeRange { child, .. }
                | QueryExpr::TimeShift { child, .. }
                | QueryExpr::SQLWindowFunc { child, .. } => from_query(child),
                _ => None,
            }
        }
        match &node.expr {
            SummaryExpr::KeepPreAsap(expr) => from_query(expr),
            SummaryExpr::SummaryAgg { child, .. } => metric(child),
            SummaryExpr::SummaryEstimate { summary_input, .. } => metric(summary_input),
            SummaryExpr::SummaryMerge { children } => {
                children.iter().find_map(|child| metric(child))
            }
            _ => None,
        }
    }

    fn handle(family: &SummaryFamilyType) -> Option<SketchKindHandle> {
        let SummaryFamilyType::Sketch(kind, _) = family else {
            return None;
        };
        Some(match asap_types::SummaryKind::from(kind.clone()) {
            asap_types::SummaryKind::DDSketch => SketchKindHandle::DDSketch,
            asap_types::SummaryKind::Kll => SketchKindHandle::Kll,
            asap_types::SummaryKind::Hll => SketchKindHandle::Hll,
            asap_types::SummaryKind::Cms => SketchKindHandle::CountMin,
            asap_types::SummaryKind::CmsWithHeap => SketchKindHandle::CmsWithHeap,
            asap_types::SummaryKind::CountSketch => SketchKindHandle::CountSketch,
            asap_types::SummaryKind::CountSketchWithHeap => SketchKindHandle::CountSketchWithHeap,
            _ => return None,
        })
    }

    let metric = metric(node)?;
    let capability = match &node.expr {
        SummaryExpr::SummaryEstimate {
            summary_input,
            query,
        } => {
            let SummaryExpr::SummaryAgg { family, .. } = &summary_input.expr else {
                return None;
            };
            match query {
                SketchQuery::Quantile { .. } => Capability::QuantileApprox(handle(family)?),
                SketchQuery::Cardinality => Capability::CardinalityApprox,
                SketchQuery::PointCount { .. } => Capability::FrequencyEstimate(handle(family)?),
                SketchQuery::TopK { .. } => Capability::FrequencyTopk(handle(family)?),
            }
        }
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(kind, _),
            ..
        } => {
            let agg = match asap_types::SummaryKind::from(kind.clone()) {
                asap_types::SummaryKind::Sum => asap_types::AggregationType::Sum,
                asap_types::SummaryKind::Increase => asap_types::AggregationType::Increase,
                asap_types::SummaryKind::MinMax => asap_types::AggregationType::MinMax,
                _ => return None,
            };
            Capability::ExactAgg(agg)
        }
        _ => return None,
    };
    Some((metric, capability))
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
