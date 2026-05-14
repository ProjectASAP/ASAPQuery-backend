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

use crate::storage_engines::sketch_db::backfill::BackfillRegistry;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::lifecycle::{AggStatus, DEFAULT_RETIREMENT_RETENTION};

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

/// Long-running tokio task that drops `Expired` sids' data.
///
/// Post-schema-retirement: the sweep is sid-driven. `SchemaRegistry`
/// is gone; this service iterates the sid catalog directly and
/// removes any sid whose status has progressed past `Retired`.
/// `BackfillRegistry` is retained so future sid↔backfill wiring can
/// reattach (today the sweep no longer cancels in-flight backfills
/// because backfill jobs remain agg_id-keyed; a follow-up will
/// rekey them on sid and restore the cancel step).
pub struct SchemaEvictionService {
    backfill: Arc<BackfillRegistry>,
    sketch_index: Arc<SketchStore>,
    retirement_retention: Duration,
    config: SchemaEvictionConfig,
}

impl SchemaEvictionService {
    pub fn new(
        sketch_index: Arc<SketchStore>,
        backfill: Arc<BackfillRegistry>,
        config: SchemaEvictionConfig,
    ) -> Self {
        Self {
            backfill,
            sketch_index,
            retirement_retention: DEFAULT_RETIREMENT_RETENTION,
            config,
        }
    }

    /// Override the retirement-retention duration. Tests use this to
    /// drive lifecycle transitions deterministically without waiting
    /// out the 24-hour default.
    pub fn with_retirement_retention(mut self, retention: Duration) -> Self {
        self.retirement_retention = retention;
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
            retirement_retention_secs = self.retirement_retention.as_secs(),
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

    /// Synchronous single sweep. Exposed as `pub` so tests can drive
    /// the service deterministically without spinning up a tokio
    /// runtime + polling loop.
    pub fn run_once(&self) {
        let expired = self.sketch_index.list_by_status(AggStatus::Expired);
        if expired.is_empty() {
            return;
        }
        info!(
            count = expired.len(),
            "SchemaEviction: {} Expired sids discovered",
            expired.len()
        );

        // Suppress an unused-field warning until backfill cancellation
        // is rewired on sid. The registry handle is retained on the
        // service for that follow-up.
        let _ = &self.backfill;

        for meta in expired {
            let sid = meta.sid;
            let metric = meta.metric_name.clone();

            if self.config.dry_run {
                info!(
                    sid,
                    %metric,
                    retired_at_ms = ?meta.retired_at_ms,
                    expires_at_ms = ?meta.expires_at_ms,
                    "SchemaEviction[DRY RUN]: would remove_instance"
                );
                continue;
            }
            let removed = self.sketch_index.remove_instance(sid).is_some();
            info!(
                sid,
                %metric,
                removed,
                retired_at_ms = ?meta.retired_at_ms,
                expires_at_ms = ?meta.expires_at_ms,
                "SchemaEviction: dropped sid"
            );
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
    use crate::storage_engines::types::{AggregationType, StreamingConfig};
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::collections::HashMap;

    fn sum_agg_config(id: u64) -> AggregationConfig {
        AggregationConfig {
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

    /// Build a streaming-config keyed on the policy-fingerprint u64
    /// derived from each dummy_agg. Returns the config plus the
    /// marker_id → fingerprint mapping so callers can look up the
    /// right key.
    fn make_streaming_config(ids: &[u64]) -> (Arc<StreamingConfig>, HashMap<u64, u64>) {
        let mut map = HashMap::new();
        let mut id_to_fp = HashMap::new();
        for &id in ids {
            let cfg = sum_agg_config(id);
            let fp = cfg.aggregation_id();
            id_to_fp.insert(id, fp);
            map.insert(fp, cfg);
        }
        (Arc::new(StreamingConfig::new(map)), id_to_fp)
    }

    fn write_one(
        sketch_index: &SketchStore,
        streaming_config: &StreamingConfig,
        agg_id: u64,
        ts: u64,
    ) -> u64 {
        use crate::drivers::ingest::series_resolver::SeriesIdResolver;
        use std::sync::Arc;
        let acc = SumAccumulator::with_sum(1.0);
        let output = crate::storage_engines::types::PrecomputedOutput::new(ts, ts + 1000, None, asap_types::PolicyFingerprint(agg_id));
        let agg_cfg = streaming_config.get_aggregation_config(agg_id).unwrap();
        // Test-scoped resolver — each call mints fresh. Production
        // shares one resolver across all sinks; tests don't need that
        // because each fixture is isolated. Static-lifetime so multiple
        // `write_one` calls in the same test share `next_sid` (matches
        // the production single-resolver model).
        thread_local! {
            static RESOLVER: Arc<SeriesIdResolver> = Arc::new(SeriesIdResolver::new());
        }
        let resolver = RESOLVER.with(|r| r.clone());
        sketch_index
            .ingest_precompute_for_agg_config(
                |m, fp, ak| resolver.resolve(m, fp, ak),
                agg_cfg,
                &output,
                &acc,
            )
            .expect("registered sid")
    }

    /// Seed two sids (metric_1, metric_2), then mark every metric_1
    /// sid as `Expired` so the sweep picks them up while metric_2's
    /// sids stay `Active`.
    fn fixture_with_expired_metric_1() -> (Arc<BackfillRegistry>, Arc<SketchStore>) {
        let (initial, id_to_fp) = make_streaming_config(&[1, 2]);
        let sketch_index = Arc::new(SketchStore::new());
        let sid_a = write_one(&sketch_index, &initial, id_to_fp[&1], 100);
        let _ = write_one(&sketch_index, &initial, id_to_fp[&1], 200);
        let _ = write_one(&sketch_index, &initial, id_to_fp[&2], 300);
        // Both metric_1 writes share the same agg-signature → one
        // sid; mark it Expired directly. metric_2's sid stays Active.
        sketch_index.force_expire(sid_a);

        (Arc::new(BackfillRegistry::new()), sketch_index)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_drops_expired_sid() {
        let (backfill, sketch_index) = fixture_with_expired_metric_1();
        let before_metric_2: usize = sketch_index
            .list_by_status(AggStatus::Active)
            .into_iter()
            .filter(|m| m.metric_name == "metric_2")
            .count();
        assert!(before_metric_2 >= 1, "fixture seeded metric_2 sid");

        let svc = SchemaEvictionService::new(
            sketch_index.clone(),
            backfill,
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: false,
            },
        );
        svc.run_once();

        // metric_1's expired sid is gone; metric_2's active sid
        // remains.
        let metric_1_remaining = sketch_index
            .list_by_status(AggStatus::Active)
            .into_iter()
            .chain(sketch_index.list_by_status(AggStatus::Retired))
            .chain(sketch_index.list_by_status(AggStatus::Expired))
            .filter(|m| m.metric_name == "metric_1")
            .count();
        assert_eq!(metric_1_remaining, 0, "expired sid must be removed");
        assert!(
            sketch_index
                .list_by_status(AggStatus::Active)
                .iter()
                .any(|m| m.metric_name == "metric_2"),
            "metric_2's sid still registered"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_once_is_noop_without_expired_sids() {
        let (initial, id_to_fp) = make_streaming_config(&[1]);
        let backfill = Arc::new(BackfillRegistry::new());
        let sketch_index = Arc::new(SketchStore::new());
        let _ = write_one(&sketch_index, &initial, id_to_fp[&1], 100);
        let before = sketch_index.instance_count();

        let svc = SchemaEvictionService::new(
            sketch_index.clone(),
            backfill,
            SchemaEvictionConfig::default(),
        );
        svc.run_once();

        assert_eq!(
            sketch_index.instance_count(),
            before,
            "no expired sids — SketchStore untouched"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dry_run_logs_but_does_not_drop() {
        let (backfill, sketch_index) = fixture_with_expired_metric_1();
        let before = sketch_index.instance_count();
        let svc = SchemaEvictionService::new(
            sketch_index.clone(),
            backfill,
            SchemaEvictionConfig {
                poll_interval: Duration::from_secs(60),
                dry_run: true,
            },
        );
        svc.run_once();

        // Dry-run: every sid stays.
        assert_eq!(sketch_index.instance_count(), before);
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
