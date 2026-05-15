pub mod workload;
pub use workload::WorkloadStore;

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::CollectionPlan;

#[derive(Debug)]
pub struct PlanStore {
    inner: RwLock<StoreInner>,
}

#[derive(Debug, Default)]
struct StoreInner {
    entries: HashMap<String, Entry>,
}

#[derive(Debug, Clone)]
struct Entry {
    current: CollectionPlan,
    previous: Option<CollectionPlan>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("plan not found for metric {0:?}")]
    NotFound(String),
    #[error("no previous plan for metric {0:?}")]
    NoPrevious(String),
}

impl PlanStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(StoreInner::default()),
        }
    }

    pub fn set(&self, metric: impl Into<String>, plan: CollectionPlan) {
        let metric = metric.into();
        let mut inner = self.inner.write().unwrap();
        match inner.entries.get_mut(&metric) {
            None => {
                inner.entries.insert(
                    metric,
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

    pub fn get(&self, metric: &str) -> Result<CollectionPlan, StoreError> {
        self.inner
            .read()
            .unwrap()
            .entries
            .get(metric)
            .map(|e| e.current.clone())
            .ok_or_else(|| StoreError::NotFound(metric.to_string()))
    }

    pub fn rollback(&self, metric: &str) -> Result<CollectionPlan, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let e = inner
            .entries
            .get_mut(metric)
            .ok_or_else(|| StoreError::NotFound(metric.to_string()))?;

        let prev = e
            .previous
            .take()
            .ok_or_else(|| StoreError::NoPrevious(metric.to_string()))?;
        e.current = prev.clone();
        e.updated_at = Utc::now();
        Ok(prev)
    }

    pub fn metrics(&self) -> Vec<String> {
        self.inner.read().unwrap().entries.keys().cloned().collect()
    }

    pub fn expired(&self, now: DateTime<Utc>) -> Vec<String> {
        self.inner
            .read()
            .unwrap()
            .entries
            .iter()
            .filter(|(_, e)| e.current.valid_until < now)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Returns a diff between the current and previous plan for `metric`.
    /// Returns `None` if no previous plan exists.
    pub fn diff(&self, metric: &str) -> Result<Option<PlanDiff>, StoreError> {
        let inner = self.inner.read().unwrap();
        let e = inner
            .entries
            .get(metric)
            .ok_or_else(|| StoreError::NotFound(metric.to_string()))?;
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
    use std::time::Duration;

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
        s.set("latency", plan.clone());
        let got = s.get("latency").unwrap();
        assert_eq!(got.valid_until, plan.valid_until);
    }

    #[test]
    fn get_not_found() {
        let s = PlanStore::new();
        assert!(matches!(s.get("missing"), Err(StoreError::NotFound(_))));
    }

    #[test]
    fn rollback() {
        let s = PlanStore::new();
        let p1 = make_plan(100);
        let p2 = make_plan(200);
        s.set("m", p1.clone());
        s.set("m", p2.clone());
        let rolled = s.rollback("m").unwrap();
        assert_eq!(rolled.valid_until, p1.valid_until);
        // After rollback, Get should return p1.
        assert_eq!(s.get("m").unwrap().valid_until, p1.valid_until);
    }

    #[test]
    fn rollback_no_previous() {
        let s = PlanStore::new();
        s.set("m", make_plan(600));
        assert!(matches!(s.rollback("m"), Err(StoreError::NoPrevious(_))));
    }

    #[test]
    fn rollback_not_found() {
        let s = PlanStore::new();
        assert!(matches!(s.rollback("x"), Err(StoreError::NotFound(_))));
    }

    #[test]
    fn metrics_list() {
        let s = PlanStore::new();
        s.set("a", make_plan(600));
        s.set("b", make_plan(600));
        let mut m = s.metrics();
        m.sort();
        assert_eq!(m, vec!["a", "b"]);
    }

    #[test]
    fn expired() {
        let s = PlanStore::new();
        s.set("old", make_plan(-1)); // already expired
        s.set("active", make_plan(600));
        let exp = s.expired(Utc::now());
        assert_eq!(exp, vec!["old"]);
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc;
        let s = Arc::new(PlanStore::new());
        s.set("m", make_plan(600));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let _ = s.get("m");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}
