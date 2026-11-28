//! The stores behind `common`'s [`common::ViewStore`] and
//! [`common::RawDataStore`], for an engine that speaks the boundary. The
//! backend's own engines use [`crate::store`] directly: queries read one
//! copy-on-write snapshot of the raw data and the views
//! ([`crate::store::PublishedStore`]), and runs read the executor's batches
//! through its `DataStore` ([`crate::store::RunStore`]), neither of which
//! the traits express.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::values::Batch;
use common::{
    LabelSet, PlanId, RawDataBatch, RawDataRetention, Source, ViewId, ViewInstance,
    ViewInstanceKey, ViewInstanceMetadata, ViewState,
};

use crate::store::{millis, RawData, RawSample, ViewStore};

/// One plan's materialized views as a [`common::ViewStore`]: a
/// [`ViewStore`] whose instances' states are a window's batches
/// (`Vec<Batch>`). Instances of another plan, and states of another type,
/// are not stored.
pub struct BoundaryViewStore {
    plan: PlanId,
    /// The plan's views by node: what each view keeps and for how long.
    views: BTreeMap<PhysicalASAPNodeId, MaterializedView>,
    store: Mutex<ViewStore>,
}

impl BoundaryViewStore {
    /// An empty store for plan `plan`, whose views are `views`.
    pub fn new(plan: PlanId, views: &[MaterializedView], store: ViewStore) -> Self {
        Self {
            plan,
            views: views.iter().map(|v| (v.node_id, v.clone())).collect(),
            store: Mutex::new(store),
        }
    }

    fn store(&self) -> std::sync::MutexGuard<'_, ViewStore> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// `batches` as a boundary instance.
fn instance(batches: Arc<Vec<Batch>>) -> ViewInstance {
    let size_bytes = batches.iter().map(|b| b.memory_bytes() as u64).sum();
    ViewInstance {
        state: ViewState(batches),
        metadata: ViewInstanceMetadata { size_bytes },
    }
}

impl common::ViewStore for BoundaryViewStore {
    fn put(&self, key: ViewInstanceKey, instance: ViewInstance) {
        let view = self.views.get(&key.view.node);
        let batches = instance.state.0.downcast_ref::<Vec<Batch>>();
        let (Some(view), Some(batches), true) = (view, batches, key.view.plan == self.plan) else {
            tracing::warn!(
                ?key,
                "not stored: another plan's view, or not a window's batches"
            );
            return;
        };
        if let Err(error) = self.store().append(view, key.window_ms, batches.clone()) {
            tracing::warn!(?key, %error, "not stored");
        }
    }

    fn get(&self, key: &ViewInstanceKey) -> Option<ViewInstance> {
        if key.view.plan != self.plan {
            return None;
        }
        let windows = self.store().windows(key.view.node, key.window_ms);
        let (_, batches) = windows.into_iter().find(|(w, _)| *w == key.window_ms)?;
        Some(instance(batches))
    }

    fn instances(&self, view: &ViewId, interval_ms: (i64, i64)) -> Vec<((i64, i64), ViewInstance)> {
        if view.plan != self.plan {
            return Vec::new();
        }
        let windows = self.store().windows(view.node, interval_ms);
        windows
            .into_iter()
            .map(|(window, batches)| (window, instance(batches)))
            .collect()
    }

    fn evict_before(&self, view: &ViewId, before_ms: i64) {
        if view.plan == self.plan {
            self.store().evict_before(view.node, before_ms);
        }
    }

    fn timestamp_range_per_view(&self) -> HashMap<ViewId, (i64, i64)> {
        let plan = self.plan;
        self.store()
            .time_ranges()
            .into_iter()
            .map(|(node, range)| (ViewId { plan, node }, range))
            .collect()
    }
}

/// Raw time series as a [`common::RawDataStore`], over a [`RawData`]: it
/// keeps the metrics `RawData` was made for, and drops any other source.
pub struct BoundaryRawDataStore {
    data: Mutex<RawData>,
}

impl BoundaryRawDataStore {
    pub fn new(data: RawData) -> Self {
        Self {
            data: Mutex::new(data),
        }
    }

    fn data(&self) -> std::sync::MutexGuard<'_, RawData> {
        self.data.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl common::RawDataStore for BoundaryRawDataStore {
    fn append(&self, source: &Source, batch: RawDataBatch) {
        // `RawData` keeps each sample by its series' `__name__`.
        if matches!(source, Source::TimeSeries { .. }) {
            self.data().append(&RawSample::from_batch(&batch));
        }
    }

    fn read(&self, source: &Source, interval_ms: (i64, i64)) -> Option<RawDataBatch> {
        let Source::TimeSeries { metric } = source else {
            return Some(RawDataBatch::TimeSeries { series: Vec::new() });
        };
        let samples = self.data().samples(metric, interval_ms)?;
        let mut by_series: BTreeMap<&LabelSet, Vec<(i64, f64)>> = BTreeMap::new();
        for sample in &samples {
            by_series
                .entry(sample.labels.labels().as_ref())
                .or_default()
                .push((sample.timestamp_ms, sample.value));
        }
        let series = by_series
            .into_iter()
            .map(|(labels, mut points)| {
                points.sort_by_key(|&(timestamp_ms, _)| timestamp_ms);
                (labels.clone(), points)
            })
            .collect();
        Some(RawDataBatch::TimeSeries { series })
    }

    fn evict(&self, retention: &[RawDataRetention], now_ms: i64) {
        let mut data = self.data();
        for retention in retention {
            let (Source::TimeSeries { metric }, Some(lookback_ms)) =
                (retention.source(), retention.lookback_ms())
            else {
                continue;
            };
            data.evict_through(metric, now_ms.saturating_sub(millis(lookback_ms)));
        }
    }
}

#[cfg(test)]
mod tests {
    use common::{RawDataStore as _, ViewStore as _};

    use super::*;

    fn view(node: PhysicalASAPNodeId) -> MaterializedView {
        serde_json::from_value(serde_json::json!({
            "node_id": node,
            "refresh": "at_ingestion",
            "plan_id": "test-plan",
            "view_state_key": {"definition_id": format!("v{node}"), "window": null,
                "logical_properties": {"parameters": null}},
            "coverage": {"definition_id": format!("v{node}"), "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap()
    }

    fn key(plan: u64, node: PhysicalASAPNodeId, window_ms: (i64, i64)) -> ViewInstanceKey {
        ViewInstanceKey {
            view: ViewId {
                plan: PlanId(plan),
                node,
            },
            window_ms,
        }
    }

    fn batches() -> ViewInstance {
        instance(Arc::new(Vec::new()))
    }

    /// Instances put are read back by key and by interval, and evicted by
    /// end; another plan's key is neither stored nor found.
    #[test]
    fn the_view_store_keeps_one_plans_windows() {
        let store = BoundaryViewStore::new(PlanId(1), &[view(3)], ViewStore::default());
        for window_ms in [(0, 10), (10, 20), (20, 30)] {
            store.put(key(1, 3, window_ms), batches());
        }
        store.put(key(2, 3, (30, 40)), batches());
        assert!(store.get(&key(1, 3, (10, 20))).is_some());
        assert!(store.get(&key(1, 3, (30, 40))).is_none());
        assert!(store.get(&key(2, 3, (10, 20))).is_none());
        let view_id = key(1, 3, (0, 0)).view;
        let windows: Vec<(i64, i64)> = store
            .instances(&view_id, (10, 30))
            .into_iter()
            .map(|(window, _)| window)
            .collect();
        assert_eq!(windows, [(10, 20), (20, 30)]);
        assert_eq!(store.timestamp_range_per_view()[&view_id], (0, 30));
        store.evict_before(&view_id, 20);
        assert_eq!(store.timestamp_range_per_view()[&view_id], (20, 30));
    }

    fn source() -> Source {
        Source::TimeSeries { metric: "m".into() }
    }

    /// Appended samples are read back per series; once evicted, a read
    /// reaching them is `None`.
    #[test]
    fn the_raw_data_store_reads_until_eviction() {
        let store = BoundaryRawDataStore::new(RawData::for_metrics([&"m".to_string()], 1000));
        let named: LabelSet = [("__name__", "m"), ("job", "a")]
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .into();
        store.append(
            &source(),
            RawDataBatch::TimeSeries {
                series: vec![(named.clone(), vec![(1000, 1.0), (2000, 2.0)])],
            },
        );
        assert_eq!(
            store.read(&source(), (0, 2000)),
            Some(RawDataBatch::TimeSeries {
                series: vec![(named.clone(), vec![(1000, 1.0), (2000, 2.0)])]
            })
        );
        let retention: RawDataRetention = serde_json::from_value(serde_json::json!({
            "source": {"TimeSeries": {"metric": "m"}}, "lookback_ms": 1000,
        }))
        .unwrap();
        store.evict(&[retention], 2500);
        assert_eq!(store.read(&source(), (0, 2000)), None);
        assert_eq!(
            store.read(&source(), (1500, 2000)),
            Some(RawDataBatch::TimeSeries {
                series: vec![(named, vec![(2000, 2.0)])]
            })
        );
    }
}
