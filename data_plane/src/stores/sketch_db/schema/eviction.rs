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
use crate::stores::sketch_db::index::SketchStore;
use super::{AggStatus, SchemaRegistry};

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
    /// Phase 5 M2.3.6g — eviction removes sids from `SketchStore`
    /// only; the legacy `Arc<dyn Store>` field is gone now that
    /// SketchStore no longer holds data (M2.3.6a) and the engine
    /// reads exclusively from SketchStore (M2.3.6f).
    sketch_index: Option<Arc<SketchStore>>,
    config: SchemaEvictionConfig,
}

impl SchemaEvictionService {
    pub fn new(
        schemas: Arc<SchemaRegistry>,
        backfill: Arc<BackfillRegistry>,
        config: SchemaEvictionConfig,
    ) -> Self {
        Self {
            schemas,
            backfill,
            sketch_index: None,
            config,
        }
    }

    /// Attach a `SketchStore` so the eviction sweep also removes the
    /// schema's per-sid state. Returns `self` (builder-style) so
    /// existing call sites can opt in with a single chained call.
    pub fn with_sketch_index(mut self, index: Arc<SketchStore>) -> Self {
        self.sketch_index = Some(index);
        self
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
            // Phase 5 M2.3.6g — drop the schema's sid state from the
            // sketch index. The legacy `store.drop_agg_id` call is
            // gone (no data lives there post-M2.3.6a).
            let removed = match self.sketch_index.as_ref() {
                Some(idx) => idx.remove_instances_for_agg_config(&schema.config),
                None => 0,
            };
            info!(
                agg_id,
                %metric,
                sids_removed = removed,
                retired_at_ms = ?schema.retired_at_ms,
                expires_at_ms = ?schema.expires_at_ms,
                running_backfills_cancelled = running.len(),
                "SchemaEviction: dropped schema"
            );

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
    use crate::precompute_engine::operators::SumAccumulator;
    use crate::stores::sketch_db::backfill::BackfillSource;
    use crate::stores::types::{AggregationType, StreamingConfig};
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

    fn write_one(
        sketch_index: &SketchStore,
        streaming_config: &StreamingConfig,
        agg_id: u64,
        ts: u64,
    ) {
        let acc = SumAccumulator::with_sum(1.0);
        let output = crate::stores::types::PrecomputedOutput::new(ts, ts + 1000, None, agg_id);
        if let Some(agg_cfg) = streaming_config.get_aggregation_config(agg_id) {
            sketch_index.ingest_precompute_for_agg_config(agg_cfg, &output, &acc);
        }
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

        let second = make_streaming_config(&[2]);
        schemas.reconcile(&second);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(schemas.get(1).unwrap().status(), AggStatus::Expired);

        let backfill = Arc::new(BackfillRegistry::new());
        let sketch_index = Arc::new(SketchStore::new());
        write_one(&sketch_index, &initial, 1, 100);
        write_one(&sketch_index, &initial, 1, 200);
        write_one(&sketch_index, &initial, 2, 300);

        (schemas, backfill, sketch_index)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_drops_expired_agg_data() {
        let (schemas, backfill, sketch_index) = fixture_with_expired_1().await;
        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: false,
            },
        )
        .with_sketch_index(sketch_index.clone());

        // Pre-condition: SketchStore has both agg's sids populated.
        assert!(sketch_index.instance_count() >= 2);

        svc.run_once();

        // Post: schema registry entry removed AND agg_1's sids gone
        // from SketchStore; agg_2's sid remains.
        assert!(
            schemas.get(1).is_none(),
            "Expired schema removed from registry"
        );
        assert!(schemas.get(2).is_some());
        let remaining: Vec<_> = sketch_index
            .list_by_status(AggStatus::Active)
            .into_iter()
            .filter(|m| m.metric_name == "metric_2")
            .collect();
        assert!(!remaining.is_empty(), "agg_2's sid still registered");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_also_removes_sketch_index_instances() {
        // M2.3.6g — fixture now writes via SketchStore directly, so
        // a separate "register a sid" step is redundant. The
        // post-condition is the same: agg_1's sids are gone after
        // the sweep.
        let (schemas, backfill, sketch_index) = fixture_with_expired_1().await;
        let before_agg1 = sketch_index
            .list_by_status(AggStatus::Active)
            .into_iter()
            .filter(|m| m.metric_name == "metric_1")
            .count();
        assert!(before_agg1 >= 1, "fixture seeded metric_1 sids");

        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: false,
            },
        )
        .with_sketch_index(sketch_index.clone());

        svc.run_once();

        let after_agg1 = sketch_index
            .list_by_status(AggStatus::Active)
            .into_iter()
            .filter(|m| m.metric_name == "metric_1")
            .count();
        assert_eq!(
            after_agg1, 0,
            "expired agg_config's sids must be removed from SketchStore"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_is_noop_with_no_expired_schemas() {
        let initial = make_streaming_config(&[1]);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&initial));
        let backfill = Arc::new(BackfillRegistry::new());
        let sketch_index = Arc::new(SketchStore::new());
        write_one(&sketch_index, &initial, 1, 100);
        let before = sketch_index.instance_count();

        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            SchemaEvictionConfig::default(),
        )
        .with_sketch_index(sketch_index.clone());
        svc.run_once();

        assert!(schemas.get(1).is_some());
        assert_eq!(
            sketch_index.instance_count(),
            before,
            "no expired schemas — SketchStore untouched"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dry_run_logs_but_does_not_drop() {
        let (schemas, backfill, sketch_index) = fixture_with_expired_1().await;
        let before = sketch_index.instance_count();
        let svc = SchemaEvictionService::new(
            schemas.clone(),
            backfill,
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: true,
            },
        )
        .with_sketch_index(sketch_index.clone());
        svc.run_once();

        // Dry-run: schema entry stays, SketchStore untouched.
        assert!(schemas.get(1).is_some());
        assert_eq!(sketch_index.instance_count(), before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancels_running_backfill_for_expired_agg() {
        let (schemas, backfill, sketch_index) = fixture_with_expired_1().await;
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
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: false,
            },
        )
        .with_sketch_index(sketch_index);
        svc.run_once();

        assert_eq!(
            backfill.get(job_id).unwrap().status,
            BackfillStatus::Cancelled
        );
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
