//! Persists the `QueryWorkload` + `WorkloadCharacteristics` associated with
//! each planned metric so that the re-planner can re-run `plan()` without
//! needing the original `QuerySpec` HTTP payload.
use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::{QueryWorkload, WorkloadCharacteristics};

pub struct WorkloadStore {
    inner: RwLock<HashMap<String, (QueryWorkload, WorkloadCharacteristics)>>,
}

impl WorkloadStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, metric: impl Into<String>, wl: QueryWorkload, wc: WorkloadCharacteristics) {
        self.inner.write().unwrap().insert(metric.into(), (wl, wc));
    }

    /// Returns a clone of `(workload, characteristics)` if the metric is known.
    pub fn get(&self, metric: &str) -> Option<(QueryWorkload, WorkloadCharacteristics)> {
        self.inner.read().unwrap().get(metric).cloned()
    }

    pub fn remove(&self, metric: &str) {
        self.inner.write().unwrap().remove(metric);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggType;
    use std::collections::HashMap;
    use std::time::Duration;

    fn wl(name: &str) -> QueryWorkload {
        QueryWorkload {
            metric_name: name.into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![AggType::Quantile],
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    #[test]
    fn set_and_get() {
        let s = WorkloadStore::new();
        s.set("latency", wl("latency"), WorkloadCharacteristics::default());
        let (got, _) = s.get("latency").unwrap();
        assert_eq!(got.metric_name, "latency");
    }

    #[test]
    fn unknown_metric_returns_none() {
        let s = WorkloadStore::new();
        assert!(s.get("nope").is_none());
    }

    #[test]
    fn overwrite_replaces() {
        let s = WorkloadStore::new();
        s.set("m", wl("m"), WorkloadCharacteristics::default());
        let mut updated = wl("m");
        updated.accuracy_sla = 0.05;
        s.set("m", updated, WorkloadCharacteristics::default());
        let (got, _) = s.get("m").unwrap();
        assert_eq!(got.accuracy_sla, 0.05);
    }

    #[test]
    fn remove_clears_entry() {
        let s = WorkloadStore::new();
        s.set("m", wl("m"), WorkloadCharacteristics::default());
        s.remove("m");
        assert!(s.get("m").is_none());
    }
}
