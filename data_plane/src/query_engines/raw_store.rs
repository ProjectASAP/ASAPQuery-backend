//! Retained input for installed typed scan nodes, shared across query roots.
use crate::drivers::ingest::prometheus_remote_write::CanonicalSample;
use crate::query_engines::asap_query_engine::logical_dag::PreparedSamples;
use crate::query_engines::EngineError;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

type Labels = BTreeMap<String, String>;
#[derive(Default)]
struct Generation {
    plan: Option<(u64, u64)>,
    series: BTreeMap<Labels, Vec<(i64, Option<f64>)>>,
    prepared: Option<Arc<PreparedSamples>>,
}

#[derive(Default)]
pub struct RawSampleStore {
    generation: Mutex<Generation>,
}

impl RawSampleStore {
    /// Called only after the receiver admits the complete batch to its workers.
    /// Receiver deduplication serializes this operation with all other admissions.
    pub fn append_admitted(
        &self,
        plan_id: u64,
        plan_version: u64,
        samples: &[CanonicalSample],
        metrics: &BTreeSet<String>,
        all_metrics: bool,
    ) {
        let mut state = self.generation.lock().unwrap_or_else(|e| e.into_inner());
        if state.plan != Some((plan_id, plan_version)) {
            *state = Generation {
                plan: Some((plan_id, plan_version)),
                ..Default::default()
            };
        }
        if metrics.is_empty() && !all_metrics {
            state.series.clear();
            state.prepared = None;
            return;
        }
        let mut changed = false;
        for sample in samples {
            if !all_metrics && !metrics.contains(&sample.metric) {
                continue;
            }
            let mut labels: Labels = sample
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            labels.insert("__name__".into(), sample.metric.clone());
            state
                .series
                .entry(labels)
                .or_default()
                .push((sample.timestamp_ms, sample.value));
            changed = true;
        }
        if changed {
            state.prepared = None;
        }
    }

    /// One immutable indexed snapshot is built per admitted input generation.
    /// Holding the lock during preparation prevents concurrent readers from
    /// redundantly indexing the same input or observing a partially admitted batch.
    pub fn snapshot(
        &self,
        plan_id: u64,
        plan_version: u64,
    ) -> Result<Arc<PreparedSamples>, EngineError> {
        let mut state = self.generation.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .plan
            .is_some_and(|plan| plan != (plan_id, plan_version))
        {
            return Err(EngineError::capability_miss(
                "local_raw_store",
                "retained input belongs to another plan generation",
            ));
        }
        if let Some(prepared) = &state.prepared {
            return Ok(prepared.clone());
        }
        let prepared = Arc::new(PreparedSamples::from_series(
            state
                .series
                .iter()
                .map(|(labels, points)| (labels.clone(), points.clone())),
        )?);
        state.prepared = Some(prepared.clone());
        Ok(prepared)
    }

    pub fn range_max_index_bytes(&self) -> usize {
        let state = self.generation.lock().unwrap_or_else(|e| e.into_inner());
        state
            .prepared
            .as_ref()
            .map_or(0, |p| p.range_max_index_bytes())
    }

    pub fn sample_count(&self) -> usize {
        self.generation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .series
            .values()
            .map(Vec::len)
            .sum()
    }

    pub fn estimated_bytes(&self) -> usize {
        let state = self.generation.lock().unwrap_or_else(|e| e.into_inner());
        state
            .series
            .iter()
            .map(|(labels, points)| {
                std::mem::size_of::<Labels>()
                    + labels
                        .iter()
                        .map(|(k, v)| {
                            k.capacity() + v.capacity() + std::mem::size_of::<(String, String)>()
                        })
                        .sum::<usize>()
                    + points.capacity() * std::mem::size_of::<(i64, Option<f64>)>()
            })
            .sum::<usize>()
            + state
                .prepared
                .as_ref()
                .map_or(0, |snapshot| snapshot.estimated_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(metric: &str, timestamp_ms: i64) -> CanonicalSample {
        CanonicalSample {
            metric: metric.into(),
            labels: Default::default(),
            series_key: metric.into(),
            timestamp_ms,
            value: Some(1.0),
        }
    }
    // Repeated queries share indexing; new admitted input publishes a coherent replacement.
    #[test]
    fn raw_store_caches_once_and_isolates_plan_generations() {
        let store = RawSampleStore::default();
        let metrics = BTreeSet::from(["a".into()]);
        store.append_admitted(1, 1, &[sample("a", 1), sample("b", 1)], &metrics, false);
        assert_eq!(store.sample_count(), 1);
        let first = store.snapshot(1, 1).unwrap();
        assert!(Arc::ptr_eq(&first, &store.snapshot(1, 1).unwrap()));
        store.append_admitted(1, 1, &[sample("a", 2)], &metrics, false);
        assert!(!Arc::ptr_eq(&first, &store.snapshot(1, 1).unwrap()));
        assert!(store.snapshot(1, 2).is_err());
        store.append_admitted(1, 2, &[], &BTreeSet::new(), false);
        assert_eq!(store.sample_count(), 0);
        assert!(store.snapshot(1, 1).is_err());
    }
}
