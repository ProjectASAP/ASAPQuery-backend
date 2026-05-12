mod common;
pub mod global;
pub mod per_key;
pub mod persistence;

use crate::stores::types::{
    AggregateCore, CleanupPolicy, LockStrategy, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use global::SketchStoreGlobal;
use per_key::SketchStorePerKey;
use std::collections::HashMap;
use std::sync::Arc;

/// Diagnostic snapshot from a single aggregation ID in the store.
pub struct AggregationDiagnostic {
    pub aggregation_id: u64,
    pub time_map_len: usize,
    pub read_counts_len: usize,
    pub num_aggregate_objects: usize,
    pub sketch_bytes: usize,
}

/// Diagnostic snapshot of the entire store.
pub struct StoreDiagnostics {
    pub num_aggregations: usize,
    pub total_time_map_entries: usize,
    pub total_sketch_bytes: usize,
    pub per_aggregation: Vec<AggregationDiagnostic>,
}

/// Enum wrapper that dispatches to either global or per-key lock implementation
pub enum SketchStore {
    Global(SketchStoreGlobal),
    PerKey(SketchStorePerKey),
}

impl SketchStore {
    /// Constructor with default strategy (backward compatibility for tests)
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self::new_with_strategy(streaming_config, cleanup_policy, LockStrategy::PerKey)
    }

    /// Collect diagnostic info for memory investigation.
    pub fn diagnostic_info(&self) -> StoreDiagnostics {
        match self {
            SketchStore::Global(store) => store.diagnostic_info(),
            SketchStore::PerKey(store) => store.diagnostic_info(),
        }
    }

    /// Constructor with explicit lock strategy (used by main.rs)
    pub fn new_with_strategy(
        streaming_config: Arc<StreamingConfig>,
        cleanup_policy: CleanupPolicy,
        lock_strategy: LockStrategy,
    ) -> Self {
        match lock_strategy {
            LockStrategy::Global => {
                SketchStore::Global(SketchStoreGlobal::new(streaming_config, cleanup_policy))
            }
            LockStrategy::PerKey => {
                SketchStore::PerKey(SketchStorePerKey::new(streaming_config, cleanup_policy))
            }
        }
    }

    /// Persistence-enabled constructor. Always returns the `PerKey`
    /// variant — persistence only targets per-key locking; the
    /// `Global` variant is intentionally left in-memory-only.
    ///
    /// Runs recovery on the disk path, starts the background flusher
    /// thread, and returns a store whose sealed epochs are flushed
    /// to disk on memory / time pressure. Query paths transparently
    /// read back from disk when in-memory state misses.
    pub fn with_persistence_per_key(
        streaming_config: Arc<StreamingConfig>,
        cleanup_policy: CleanupPolicy,
        persistence_cfg: persistence::SketchStorePersistenceConfig,
    ) -> persistence::PersistResult<Self> {
        Ok(SketchStore::PerKey(
            SketchStorePerKey::with_persistence(
                streaming_config,
                cleanup_policy,
                persistence_cfg,
            )?,
        ))
    }
}

#[async_trait::async_trait]
impl Store for SketchStore {
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> StoreResult<()> {
        match self {
            SketchStore::Global(store) => store.insert_precomputed_output(output, precompute),
            SketchStore::PerKey(store) => store.insert_precomputed_output(output, precompute),
        }
    }

    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        match self {
            SketchStore::Global(store) => store.insert_precomputed_output_batch(outputs),
            SketchStore::PerKey(store) => store.insert_precomputed_output_batch(outputs),
        }
    }

    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SketchStore::Global(store) => {
                store.query_precomputed_output(metric, aggregation_id, start, end)
            }
            SketchStore::PerKey(store) => {
                store.query_precomputed_output(metric, aggregation_id, start, end)
            }
        }
    }

    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SketchStore::Global(store) => {
                store.query_precomputed_output_exact(metric, aggregation_id, exact_start, exact_end)
            }
            SketchStore::PerKey(store) => {
                store.query_precomputed_output_exact(metric, aggregation_id, exact_start, exact_end)
            }
        }
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SketchStore::Global(store) => store.get_earliest_timestamp_per_aggregation_id(),
            SketchStore::PerKey(store) => store.get_earliest_timestamp_per_aggregation_id(),
        }
    }

    fn close(&self) -> StoreResult<()> {
        match self {
            SketchStore::Global(store) => store.close(),
            SketchStore::PerKey(store) => store.close(),
        }
    }

    fn drop_agg_id(&self, agg_id: u64) -> StoreResult<usize> {
        match self {
            SketchStore::Global(store) => store.drop_agg_id(agg_id),
            SketchStore::PerKey(store) => store.drop_agg_id(agg_id),
        }
    }
}

#[cfg(test)]
mod drop_agg_id_tests {
    use super::*;
    use crate::stores::types::AggregationType;
    use crate::precompute_engine::operators::SumAccumulator;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

    fn two_agg_streaming_config() -> Arc<StreamingConfig> {
        let cfg = |id: u64| AggregationConfig {
            aggregation_id: id,
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1,
            slide_interval: 1,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: format!("metric_{id}"),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        let mut map = HashMap::new();
        map.insert(1u64, cfg(1));
        map.insert(2u64, cfg(2));
        Arc::new(StreamingConfig::new(map))
    }

    fn write_one(store: &SketchStore, agg_id: u64, value: f64, ts: u64) {
        let acc = SumAccumulator::with_sum(value);
        let output = PrecomputedOutput::new(ts, ts + 1000, None, agg_id);
        store
            .insert_precomputed_output(output, Box::new(acc))
            .expect("insert ok");
    }

    fn total_buckets(store: &SketchStore, metric: &str, agg_id: u64) -> usize {
        let map = store
            .query_precomputed_output(metric, agg_id, 0, u64::MAX / 2)
            .expect("query ok");
        map.values().map(|v| v.len()).sum()
    }

    fn make_store(strategy: LockStrategy) -> SketchStore {
        SketchStore::new_with_strategy(
            two_agg_streaming_config(),
            CleanupPolicy::NoCleanup,
            strategy,
        )
    }

    #[test]
    fn global_drop_removes_only_target_agg() {
        let store = make_store(LockStrategy::Global);
        write_one(&store, 1, 10.0, 0);
        write_one(&store, 1, 20.0, 1_000);
        write_one(&store, 1, 30.0, 2_000);
        write_one(&store, 2, 99.0, 5_000);

        assert_eq!(total_buckets(&store, "metric_1", 1), 3);
        assert_eq!(total_buckets(&store, "metric_2", 2), 1);

        let evicted = store.drop_agg_id(1).expect("drop ok");
        assert_eq!(evicted, 3);

        assert_eq!(total_buckets(&store, "metric_1", 1), 0);
        assert_eq!(total_buckets(&store, "metric_2", 2), 1);
    }

    #[test]
    fn global_drop_unknown_agg_is_noop_returns_zero() {
        let store = make_store(LockStrategy::Global);
        let evicted = store.drop_agg_id(999).expect("drop ok");
        assert_eq!(evicted, 0);
    }

    #[test]
    fn global_drop_clears_earliest_timestamp_index() {
        let store = make_store(LockStrategy::Global);
        write_one(&store, 1, 10.0, 100);
        let before = store.get_earliest_timestamp_per_aggregation_id().unwrap();
        assert!(before.contains_key(&1));

        store.drop_agg_id(1).unwrap();

        let after = store.get_earliest_timestamp_per_aggregation_id().unwrap();
        assert!(!after.contains_key(&1));
    }

    #[test]
    fn per_key_drop_removes_only_target_agg() {
        let store = make_store(LockStrategy::PerKey);
        write_one(&store, 1, 10.0, 0);
        write_one(&store, 1, 20.0, 1_000);
        write_one(&store, 2, 99.0, 5_000);

        assert_eq!(total_buckets(&store, "metric_1", 1), 2);
        assert_eq!(total_buckets(&store, "metric_2", 2), 1);

        let evicted = store.drop_agg_id(1).expect("drop ok");
        assert_eq!(evicted, 2);

        assert_eq!(total_buckets(&store, "metric_1", 1), 0);
        assert_eq!(total_buckets(&store, "metric_2", 2), 1);
    }

    #[test]
    fn per_key_drop_unknown_agg_is_noop() {
        let store = make_store(LockStrategy::PerKey);
        let evicted = store.drop_agg_id(999).expect("drop ok");
        assert_eq!(evicted, 0);
    }

    #[test]
    fn per_key_drop_clears_earliest_timestamp_index() {
        let store = make_store(LockStrategy::PerKey);
        write_one(&store, 1, 10.0, 100);
        let before = store.get_earliest_timestamp_per_aggregation_id().unwrap();
        assert!(before.contains_key(&1));

        store.drop_agg_id(1).unwrap();

        let after = store.get_earliest_timestamp_per_aggregation_id().unwrap();
        assert!(!after.contains_key(&1));
    }

    #[test]
    fn drop_then_reinsert_behaves_as_fresh_agg() {
        let store = make_store(LockStrategy::Global);
        write_one(&store, 1, 10.0, 100);
        store.drop_agg_id(1).unwrap();

        write_one(&store, 1, 42.0, 500);
        assert_eq!(total_buckets(&store, "metric_1", 1), 1);
        let ts_map = store.get_earliest_timestamp_per_aggregation_id().unwrap();
        assert_eq!(ts_map.get(&1).copied(), Some(500));
    }
}
