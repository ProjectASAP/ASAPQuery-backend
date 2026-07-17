pub mod workload;
pub use workload::{WorkloadKey, WorkloadStore};

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::CollectionPlan;
use crate::workload::AggRole;

/// Composite key `(metric_name, role)` for the plan store — same
/// shape as [`WorkloadKey`]. See [`crate::workload::AggRole`] for
/// the B2 restructure rationale.
pub type PlanKey = (String, AggRole);

#[derive(Debug)]
pub struct PlanStore {
    inner: RwLock<StoreInner>,
}

#[derive(Debug, Default)]
struct StoreInner {
    entries: HashMap<PlanKey, Entry>,
}

#[derive(Debug, Clone)]
struct Entry {
    current: CollectionPlan,
    previous: Option<CollectionPlan>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("plan not found for metric {0:?} role {1:?}")]
    NotFound(String, AggRole),
    #[error("no previous plan for metric {0:?} role {1:?}")]
    NoPrevious(String, AggRole),
}

impl PlanStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(StoreInner::default()),
        }
    }

    /// Insert or replace the plan for `(metric, role)`. When a prior
    /// plan exists, it is preserved as `previous` so [`Self::rollback`]
    /// can restore it.
    pub fn set(&self, metric: impl Into<String>, role: AggRole, plan: CollectionPlan) {
        let key: PlanKey = (metric.into(), role);
        let mut inner = self.inner.write().unwrap();
        match inner.entries.get_mut(&key) {
            None => {
                inner.entries.insert(
                    key,
                    Entry {
                        current: plan,
                        previous: None,
                        updated_at: Utc::now(),
                    },
                );
            }
            Some(e) => {
                let prev = e.current.clone();
                e.previous = Some(prev);
                e.current = plan;
                e.updated_at = Utc::now();
            }
        }
    }

    pub fn get(&self, metric: &str, role: AggRole) -> Result<CollectionPlan, StoreError> {
        self.inner
            .read()
            .unwrap()
            .entries
            .get(&(metric.to_string(), role))
            .map(|e| e.current.clone())
            .ok_or_else(|| StoreError::NotFound(metric.to_string(), role))
    }

    /// Returns all `(role, plan)` pairs for `metric` across every role.
    /// Empty vec when no role has a plan registered for the metric.
    pub fn get_all_for_metric(&self, metric: &str) -> Vec<(AggRole, CollectionPlan)> {
        self.inner
            .read()
            .unwrap()
            .entries
            .iter()
            .filter(|((m, _), _)| m == metric)
            .map(|((_, role), e)| (*role, e.current.clone()))
            .collect()
    }

    pub fn rollback(&self, metric: &str, role: AggRole) -> Result<CollectionPlan, StoreError> {
        let key: PlanKey = (metric.to_string(), role);
        let mut inner = self.inner.write().unwrap();
        let e = inner
            .entries
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(metric.to_string(), role))?;

        let prev = e
            .previous
            .take()
            .ok_or_else(|| StoreError::NoPrevious(metric.to_string(), role))?;
        e.current = prev.clone();
        e.updated_at = Utc::now();
        Ok(prev)
    }

    /// Returns every `(metric, role)` key currently in the store.
    pub fn keys(&self) -> Vec<PlanKey> {
        self.inner.read().unwrap().entries.keys().cloned().collect()
    }

    /// Returns every distinct metric name currently in the store
    /// (dedup'd across roles). Used by the metrics-exposer's plan-id
    /// gauge which aggregates per metric.
    pub fn metrics(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .inner
            .read()
            .unwrap()
            .entries
            .keys()
            .map(|(m, _)| m.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Returns every `(metric, role)` whose `valid_until` is in the past.
    pub fn expired(&self, now: DateTime<Utc>) -> Vec<PlanKey> {
        self.inner
            .read()
            .unwrap()
            .entries
            .iter()
            .filter(|(_, e)| e.current.valid_until < now)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Returns a diff between the current and previous plan for the
    /// `(metric, role)` pair. Returns `None` if no previous plan exists.
    pub fn diff(&self, metric: &str, role: AggRole) -> Result<Option<PlanDiff>, StoreError> {
        let inner = self.inner.read().unwrap();
        let e = inner
            .entries
            .get(&(metric.to_string(), role))
            .ok_or_else(|| StoreError::NotFound(metric.to_string(), role))?;
        let Some(prev) = &e.previous else {
            return Ok(None);
        };
        let curr = &e.current;
        let diff = PlanDiff {
            sketch_type_changed: prev.agent_config.sketch_type != curr.agent_config.sketch_type,
            prev_sketch_type: prev.agent_config.sketch_type.to_string(),
            curr_sketch_type: curr.agent_config.sketch_type.to_string(),
            delta_transmission_changed: prev.agent_config.delta_transmission
                != curr.agent_config.delta_transmission,
            prev_delta_transmission: prev.agent_config.delta_transmission,
            curr_delta_transmission: curr.agent_config.delta_transmission,
            mode_changed: prev.agent_config.mode != curr.agent_config.mode,
            prev_mode: prev.agent_config.mode.to_string(),
            curr_mode: curr.agent_config.mode.to_string(),
            updated_at: e.updated_at,
        };
        Ok(Some(diff))
    }
}

/// A human-readable summary of what changed between the current and previous plan.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PlanDiff {
    pub sketch_type_changed: bool,
    pub prev_sketch_type: String,
    pub curr_sketch_type: String,
    pub delta_transmission_changed: bool,
    pub prev_delta_transmission: bool,
    pub curr_delta_transmission: bool,
    pub mode_changed: bool,
    pub prev_mode: String,
    pub curr_mode: String,
    pub updated_at: DateTime<Utc>,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    fn make_plan(valid_secs: i64) -> CollectionPlan {
        let valid_until = Utc::now() + chrono::Duration::seconds(valid_secs);
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: SketchType::DDSketch,
                sketch_params: Default::default(),
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: None,
                mode: ProcessorMode::Batch,
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: false,
                series_id_ttl_secs: 300,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            precompute: vec![],
            valid_until,
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    #[test]
    fn set_and_get() {
        let s = PlanStore::new();
        let plan = make_plan(600);
        s.set("latency", AggRole::Quantile, plan.clone());
        let got = s.get("latency", AggRole::Quantile).unwrap();
        assert_eq!(got.valid_until, plan.valid_until);
    }

    #[test]
    fn get_not_found() {
        let s = PlanStore::new();
        assert!(matches!(
            s.get("missing", AggRole::Quantile),
            Err(StoreError::NotFound(..))
        ));
    }

    #[test]
    fn rollback() {
        let s = PlanStore::new();
        let p1 = make_plan(100);
        let p2 = make_plan(200);
        s.set("m", AggRole::Quantile, p1.clone());
        s.set("m", AggRole::Quantile, p2.clone());
        let rolled = s.rollback("m", AggRole::Quantile).unwrap();
        assert_eq!(rolled.valid_until, p1.valid_until);
        // After rollback, Get should return p1.
        assert_eq!(
            s.get("m", AggRole::Quantile).unwrap().valid_until,
            p1.valid_until
        );
    }

    #[test]
    fn rollback_no_previous() {
        let s = PlanStore::new();
        s.set("m", AggRole::Quantile, make_plan(600));
        assert!(matches!(
            s.rollback("m", AggRole::Quantile),
            Err(StoreError::NoPrevious(..))
        ));
    }

    #[test]
    fn rollback_not_found() {
        let s = PlanStore::new();
        assert!(matches!(
            s.rollback("x", AggRole::Quantile),
            Err(StoreError::NotFound(..))
        ));
    }

    #[test]
    fn metrics_dedups_across_roles() {
        let s = PlanStore::new();
        s.set("a", AggRole::Quantile, make_plan(600));
        s.set("a", AggRole::Sum, make_plan(600));
        s.set("b", AggRole::Sum, make_plan(600));
        let m = s.metrics();
        assert_eq!(m, vec!["a", "b"]);
    }

    #[test]
    fn expired() {
        let s = PlanStore::new();
        s.set("old", AggRole::Quantile, make_plan(-1)); // already expired
        s.set("active", AggRole::Sum, make_plan(600));
        let exp = s.expired(Utc::now());
        assert_eq!(exp, vec![("old".to_string(), AggRole::Quantile)]);
    }

    #[test]
    fn different_roles_for_same_metric_coexist() {
        // The PlanStore's role-keyed mirror of WorkloadStore's
        // same-named test. Pins the B2 contract: per-role plans
        // persist independently.
        let s = PlanStore::new();
        let plan_q = make_plan(600);
        let plan_s = make_plan(700);
        let plan_c = make_plan(800);
        s.set("http_requests_total", AggRole::Quantile, plan_q.clone());
        s.set("http_requests_total", AggRole::Sum, plan_s.clone());
        s.set("http_requests_total", AggRole::Count, plan_c.clone());

        assert_eq!(
            s.get("http_requests_total", AggRole::Quantile)
                .unwrap()
                .valid_until,
            plan_q.valid_until
        );
        assert_eq!(
            s.get("http_requests_total", AggRole::Sum)
                .unwrap()
                .valid_until,
            plan_s.valid_until
        );
        assert_eq!(
            s.get("http_requests_total", AggRole::Count)
                .unwrap()
                .valid_until,
            plan_c.valid_until
        );

        let all = s.get_all_for_metric("http_requests_total");
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc;
        let s = Arc::new(PlanStore::new());
        s.set("m", AggRole::Quantile, make_plan(600));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let _ = s.get("m", AggRole::Quantile);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}
