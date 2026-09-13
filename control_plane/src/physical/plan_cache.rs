/// Cached physical deployment-plan compiler.
///
/// A *baseline* is the cost-optimised `CollectionPlan` established on the
/// **first** `POST /api/v1/plan` request for a metric.  Once set, the same
/// plan is returned for every subsequent request — even if the workload
/// characteristics change — giving a stable, predictable collector
/// configuration in production.
///
/// The baseline is intentionally static: live workload fluctuations do **not**
/// trigger re-optimisation, which prevents mid-stream sketch-type flips that
/// would break downstream aggregation pipelines.
///
/// To replace the baseline (e.g. after an SLA violation or explicit rollback)
/// call [`CachedDeploymentPlanner::reset`] for the metric.  The next plan request will
/// run the cost model afresh and lock in a new baseline.
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::physical::deployment_cost::DeploymentCostPlanner;
use crate::types::{CollectionPlan, LegacyMetricWorkload, WorkloadCharacteristics};

pub struct CachedDeploymentPlanner {
    inner: DeploymentCostPlanner,
    cache: Arc<RwLock<HashMap<String, CollectionPlan>>>,
}

impl CachedDeploymentPlanner {
    pub fn new(inner: DeploymentCostPlanner) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Return the baseline plan for this metric, or run the cost model and
    /// establish a new baseline if this is the first request for the metric.
    pub fn plan(
        &self,
        workload: &LegacyMetricWorkload,
        wc: Option<&WorkloadCharacteristics>,
    ) -> CollectionPlan {
        let key = &workload.metric_name;

        // Fast path: return the cached plan if one exists.
        {
            let cache = self.cache.read().unwrap();
            if let Some(plan) = cache.get(key) {
                return plan.clone();
            }
        }

        // Slow path: first request for this metric — run cost optimisation.
        let plan = self.inner.plan(workload, wc);
        self.cache
            .write()
            .unwrap()
            .insert(key.clone(), plan.clone());
        plan
    }

    /// Clear the baseline for a metric so the next request
    /// triggers a fresh cost-model run.  Called by the rollback handler or
    /// any future re-plan endpoint.
    pub fn reset(&self, metric: &str) {
        self.cache.write().unwrap().remove(metric);
    }

    /// Return the metric names that have an established baseline.
    pub fn baseline_metrics(&self) -> Vec<String> {
        self.cache.read().unwrap().keys().cloned().collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggType;
    use std::collections::HashMap;
    use std::time::Duration;

    fn workload(metric: &str) -> LegacyMetricWorkload {
        LegacyMetricWorkload {
            metric_name: metric.into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![AggType::Quantile],
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![0.99],
        }
    }

    fn planner() -> CachedDeploymentPlanner {
        CachedDeploymentPlanner::new(DeploymentCostPlanner::new())
    }

    #[test]
    fn first_call_produces_a_plan() {
        let p = planner();
        let plan = p.plan(&workload("latency"), None);
        // Cost model picks the cheapest sketch that meets the SLA; verify
        // we got a valid plan.  transmit_sketch defaults to false (enabled
        // by DeploymentCostPlanner when appropriate).
        assert!(!plan.agent_config.transmit_sketch);
    }

    #[test]
    fn second_call_returns_same_plan() {
        let p = planner();
        let first = p.plan(&workload("latency"), None);
        // Change the workload — the baseline planner must ignore it.
        let mut w2 = workload("latency");
        w2.aggregations = vec![AggType::Cardinality];
        let second = p.plan(&w2, None);
        assert_eq!(
            first.agent_config.sketch_type, second.agent_config.sketch_type,
            "baseline plan must not change even when workload changes"
        );
    }

    #[test]
    fn different_metrics_get_independent_plans() {
        let p = planner();
        let a = p.plan(&workload("metric_a"), None);
        let b = p.plan(&workload("metric_b"), None);
        // Both plans are valid (exact sketch type may differ by cost model
        // internals, but we just check they are independently produced).
        let _ = (a, b);
        assert_eq!(p.baseline_metrics().len(), 2);
    }

    #[test]
    fn reset_allows_re_plan() {
        let p = planner();
        let first = p.plan(&workload("latency"), None);
        p.reset("latency");
        assert!(p.baseline_metrics().is_empty());
        // After reset the planner will run the cost model again on the same
        // workload and should produce an equivalent plan.
        let second = p.plan(&workload("latency"), None);
        assert_eq!(
            first.agent_config.sketch_type, second.agent_config.sketch_type,
            "same workload after reset should produce the same sketch type"
        );
    }

    #[test]
    fn baseline_metrics_lists_all_seen_metrics() {
        let p = planner();
        p.plan(&workload("cpu"), None);
        p.plan(&workload("mem"), None);
        p.plan(&workload("cpu"), None); // repeat — should not double-count
        let mut metrics = p.baseline_metrics();
        metrics.sort();
        assert_eq!(metrics, vec!["cpu", "mem"]);
    }
}
