//! `SchemaEvictionService` — background task that drops
//! `AggStatus::Expired` schemas' data and removes them from the
//! registry.
//!
//! Implements the §6.2 "scheduled for deletion by the time-TTL
//! sweep" semantics the lifecycle enum promises. Sits alongside
//! the SketchStore's age-based `persistence_delete_older_than`
//! retention — the two are independent:
//!
//! * **Schema retention** (this module): lifecycle-driven. When a
//!   schema is removed from `StreamingConfig` it transitions
//!   `Active → Retired → Expired`; when `expires_at_ms` passes we
//!   drop its `agg_id`.
//! * **Data retention** (SketchStore): age-driven. Records
//!   older than `persistence_delete_older_than` get swept up
//!   regardless of schema.
//!
//! ## Ordering guideline
//!
//! The user's design rule is: `persistence_delete_older_than >
//! retirement_retention`. That way data-retention never beats
//! schema-eviction to the punch on an Expired schema's records —
//! schema-eviction takes them out cleanly in one bulk drop
//! (O(1)-ish per the `drop_agg_id` contract) before data-retention
//! would wade in record-by-record. `SchemaEvictionService` logs a
//! `warn!` at startup if the ordering is inverted.
//!
//! ## What the service does on each tick
//!
//! 1. Snapshot the schema registry: collect every `AggStatus::Expired`
//!    schema.
//! 2. For each Expired schema's `agg_id`:
//!    - Cancel any `Running` backfill job targeting that agg_id
//!      (they're writing to data about to be dropped — wasted work).
//!    - Call `store.drop_agg_id(agg_id)` to evict the windows.
//!    - `schema_registry.remove_schema(agg_id)` to drop the registry
//!      entry so it won't be re-evicted next tick.
//! 3. Log an audit line per eviction with `agg_id`, metric,
//!    `retired_at_ms`, windows evicted.
//!
//! ## Dry-run
//!
//! `--schema-eviction-dry-run` sets `dry_run: true`. Every step
//! above runs through the discovery + logging, but `drop_agg_id`
//! and `remove_schema` are skipped. Use this to validate a new
//! retention value before letting it delete anything.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::stores::sketch_db::backfill::{BackfillRegistry, BackfillStatus};
use super::{AggStatus, SchemaRegistry};
use crate::stores::traits::Store;

/// Configuration for the eviction loop. Separate from
/// `SchemaRegistry`'s `retirement_retention` because the service
/// owns the poll schedule, not the data model.
#[derive(Clone, Debug)]
pub struct SchemaEvictionConfig {
    /// How often to scan for Expired schemas. Coarse (default 5 min)
    /// — eviction is not latency-sensitive.
    pub poll_interval: Duration,
    /// When true, log what would be evicted but don't call
    /// `drop_agg_id` or `remove_schema`. Use for staging / validation.
    pub dry_run: bool,
}

impl Default for SchemaEvictionConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(300),
            dry_run: false,
        }
    }
}

/// Long-running tokio task that drops Expired schemas' data.
pub struct SchemaEvictionService {
    schemas: Arc<SchemaRegistry>,
    backfill: Arc<BackfillRegistry>,
    store: Arc<dyn Store>,
    config: SchemaEvictionConfig,
}

impl SchemaEvictionService {
    pub fn new(
        schemas: Arc<SchemaRegistry>,
        backfill: Arc<BackfillRegistry>,
        store: Arc<dyn Store>,
        config: SchemaEvictionConfig,
    ) -> Self {
        Self {
            schemas,
            backfill,
            store,
            config,
        }
    }

    /// Spawn as a tokio task. Returns a handle whose `shutdown`
    /// oneshot stops the loop cleanly on ctrl-c.
    pub fn spawn(self) -> SchemaEvictionHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });
        SchemaEvictionHandle {
            task: Some(task),
            shutdown: Some(shutdown_tx),
        }
    }

    /// Main loop. Exits when `shutdown` fires.
    async fn run(self, mut shutdown: oneshot::Receiver<()>) {
        info!(
            poll_interval_secs = self.config.poll_interval.as_secs(),
            dry_run = self.config.dry_run,
            retirement_retention_secs = self.schemas.retirement_retention().as_secs(),
            "SchemaEvictionService starting"
        );
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("SchemaEvictionService received shutdown signal");
                    return;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }
            self.run_once();
        }
    }

    /// Synchronous single sweep. Exposed as `pub(crate)` so tests
    /// can drive the service deterministically without spinning up
    /// a tokio runtime + polling loop.
    pub fn run_once(&self) {
        let expired = self.schemas.list_by_status(AggStatus::Expired);
        if expired.is_empty() {
            return;
        }
        info!(
            count = expired.len(),
            "SchemaEviction: {} Expired schemas discovered",
            expired.len()
        );

        for schema in expired {
            let agg_id = schema.agg_id;
            let metric = schema.metric_name.clone();

            // Step 1: cancel in-flight backfills for this agg_id.
            let running: Vec<u64> = self
                .backfill
                .list_by_status(&BackfillStatus::Running)
                .into_iter()
                .filter(|j| j.agg_id == agg_id)
                .map(|j| j.job_id)
                .collect();
            for job_id in &running {
                if self.config.dry_run {
                    info!(
                        agg_id,
                        %metric,
                        job_id,
                        "SchemaEviction[DRY RUN]: would cancel running backfill"
                    );
                } else {
                    self.backfill.cancel(*job_id);
                    info!(agg_id, %metric, job_id, "SchemaEviction: cancelled running backfill");
                }
            }

            // Step 2: drop the data.
            if self.config.dry_run {
                info!(
                    agg_id,
                    %metric,
                    retired_at_ms = ?schema.retired_at_ms,
                    expires_at_ms = ?schema.expires_at_ms,
                    "SchemaEviction[DRY RUN]: would drop_agg_id + remove_schema"
                );
                continue;
            }
            match self.store.drop_agg_id(agg_id) {
                Ok(evicted_windows) => {
                    info!(
                        agg_id,
                        %metric,
                        evicted_windows,
                        retired_at_ms = ?schema.retired_at_ms,
                        expires_at_ms = ?schema.expires_at_ms,
                        running_backfills_cancelled = running.len(),
                        "SchemaEviction: dropped agg_id"
                    );
                }
                Err(e) => {
                    warn!(
                        agg_id,
                        %metric,
                        error = %e,
                        "SchemaEviction: drop_agg_id failed; leaving schema in registry for retry"
                    );
                    continue;
                }
            }

            // Step 3: remove the schema record.
            self.schemas.remove_schema(agg_id);
        }
    }
}

/// Handle returned by `SchemaEvictionService::spawn`. Drop triggers
/// shutdown; call `shutdown().await` to also await task exit.
pub struct SchemaEvictionHandle {
    task: Option<JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl SchemaEvictionHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for SchemaEvictionHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Log a warning if `data_retention` is shorter than
/// `retirement_retention`. The design expects the opposite
/// (`data_retention > retirement_retention`) so that schema
/// eviction runs strictly before age-based retention can beat it
/// to an Expired agg's records. Inversion isn't a hard error —
/// test setups may want it — just worth surfacing once at startup.
pub fn warn_if_retention_inverted(
    data_retention: Option<Duration>,
    retirement_retention: Duration,
) {
    if let Some(d) = data_retention {
        if d < retirement_retention {
            warn!(
                data_retention_secs = d.as_secs(),
                retirement_retention_secs = retirement_retention.as_secs(),
                "persistence_delete_older_than < schema retirement_retention; \
                 data retention may race schema eviction. Consider extending \
                 persistence_delete_older_than to at least the retirement window."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::types::{AggregationType, CleanupPolicy, LockStrategy, StreamingConfig};
    use crate::precompute_engine::operators::SumAccumulator;
    use crate::stores::sketch_db::{backfill::BackfillSource, store::SketchStore};
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::collections::HashMap;

    fn sum_agg_config(id: u64) -> AggregationConfig {
        AggregationConfig {
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
            table_name: None,
            value_column: None,
        }
    }

    fn make_streaming_config(ids: &[u64]) -> Arc<StreamingConfig> {
        let mut map = HashMap::new();
        for &id in ids {
            map.insert(id, sum_agg_config(id));
        }
        Arc::new(StreamingConfig::new(map))
    }

    fn write_one(store: &SketchStore, agg_id: u64, ts: u64) {
        let acc = SumAccumulator::with_sum(1.0);
        let output = crate::stores::types::PrecomputedOutput::new(ts, ts + 1000, None, agg_id);
        store
            .insert_precomputed_output(output, Box::new(acc))
            .unwrap();
    }

    fn total_buckets(store: &SketchStore, metric: &str, agg_id: u64) -> usize {
        let map = store
            .query_precomputed_output(metric, agg_id, 0, u64::MAX / 2)
            .unwrap();
        map.values().map(|v| v.len()).sum()
    }

    /// Build a service + registries where `agg_id=1` is already
    /// Expired (retention set tiny, reconciled out, sleep past
    /// expiry) and `agg_id=2` is still Active.
    async fn fixture_with_expired_1() -> (
        Arc<SchemaRegistry>,
        Arc<BackfillRegistry>,
        Arc<SketchStore>,
    ) {
        let initial = make_streaming_config(&[1, 2]);
        let mut registry = SchemaRegistry::from_streaming_config(&initial);
        registry.set_retention(Duration::from_millis(20));
        let schemas = Arc::new(registry);

        // Retire agg 1 (simulate a reconfigure that removed it).
        let second = make_streaming_config(&[2]);
        schemas.reconcile(&second);
        // Wait past expiry.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(schemas.get(1).unwrap().status(), AggStatus::Expired);

        let backfill = Arc::new(BackfillRegistry::new());
        let store = Arc::new(SketchStore::new_with_strategy(
            initial,
            CleanupPolicy::NoCleanup,
            LockStrategy::Global,
        ));

        // Put data under both agg_ids.
        write_one(&store, 1, 100);
        write_one(&store, 1, 200);
        write_one(&store, 2, 300);

        (schemas, backfill, store)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_drops_expired_agg_data() {
        let (schemas, backfill, store) = fixture_with_expired_1().await;
        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            store.clone(),
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: false,
            },
        );

        assert_eq!(total_buckets(&store, "metric_1", 1), 2);
        assert_eq!(total_buckets(&store, "metric_2", 2), 1);

        svc.run_once();

        // Expired agg's data gone, active agg's data untouched.
        assert_eq!(total_buckets(&store, "metric_1", 1), 0);
        assert_eq!(total_buckets(&store, "metric_2", 2), 1);
        // Schema registry entry is removed too.
        assert!(
            schemas.get(1).is_none(),
            "Expired schema removed from registry"
        );
        assert!(schemas.get(2).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_is_noop_with_no_expired_schemas() {
        let initial = make_streaming_config(&[1]);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&initial));
        let backfill = Arc::new(BackfillRegistry::new());
        let store = Arc::new(SketchStore::new_with_strategy(
            initial,
            CleanupPolicy::NoCleanup,
            LockStrategy::Global,
        ));
        write_one(&store, 1, 100);

        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            store.clone(),
            SchemaEvictionConfig::default(),
        );
        svc.run_once();

        // Active schema untouched.
        assert!(schemas.get(1).is_some());
        assert_eq!(total_buckets(&store, "metric_1", 1), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dry_run_logs_but_does_not_drop() {
        let (schemas, backfill, store) = fixture_with_expired_1().await;
        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            store.clone(),
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: true,
            },
        );
        svc.run_once();
        // Data still there, schema still there.
        assert_eq!(total_buckets(&store, "metric_1", 1), 2);
        assert!(schemas.get(1).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancels_running_backfill_for_expired_agg() {
        let (schemas, backfill, store) = fixture_with_expired_1().await;
        // Queue + start a backfill for agg 1 (the one about to be
        // evicted). Even though create_checked would reject (agg is
        // Retired/Expired, not Active), use the raw `create` for
        // the test — simulates a stale job the eviction should
        // clean up.
        let job_id = backfill.create(
            1,
            (0, 50),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );
        assert!(backfill.start(job_id));
        assert_eq!(
            backfill.get(job_id).unwrap().status,
            BackfillStatus::Running
        );

        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill.clone(),
            store.clone(),
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: false,
            },
        );
        svc.run_once();

        // Running backfill got cancelled.
        assert_eq!(
            backfill.get(job_id).unwrap().status,
            BackfillStatus::Cancelled
        );
        // Agg data + schema dropped as normal.
        assert_eq!(total_buckets(&store, "metric_1", 1), 0);
        assert!(schemas.get(1).is_none());
    }

    #[test]
    fn retention_inverted_warning_fires_only_when_inverted() {
        // Pure call-it-and-nothing-panics check. The warn goes to
        // tracing; we can't assert on the log without a subscriber
        // mock, but we can confirm the function doesn't panic on
        // either branch.
        warn_if_retention_inverted(Some(Duration::from_secs(1_000)), Duration::from_secs(100)); // Not inverted — no warn.
        warn_if_retention_inverted(Some(Duration::from_secs(100)), Duration::from_secs(1_000)); // Inverted — warn.
        warn_if_retention_inverted(None, Duration::from_secs(1_000)); // Skip.
    }
}
