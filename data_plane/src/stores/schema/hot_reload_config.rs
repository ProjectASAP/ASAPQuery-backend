//! Hot-reloadable `StreamingConfig` state.
//!
//! Wraps a shared `StreamingConfig` in `arc_swap::ArcSwap` so an
//! external controller can push a new config at runtime via
//! `POST /api/v1/streaming-config` without restarting the query
//! engine binary.
//!
//! ## How the pieces see the swap
//!
//! All three readers share clones of the same `HotReloadStreamingConfig`
//! handle (internally `Arc<ArcSwap<StreamingConfig>>`), so they
//! observe the swap at the same instant:
//!
//! * **Writes** — atomic via `ArcSwap::store`. Lock-free; readers that
//!   hold a stale snapshot finish their work with the old config and
//!   drop it when the last reference goes out of scope.
//! * **ASAPQueryEngine** — re-snapshots per query
//!   (`streaming_config_snapshot()`). New aggregations are
//!   query-matchable immediately after the swap lands.
//! * **IngestState** — re-snapshots per ingest batch
//!   (`config_snapshot()`). New aggregations start receiving data on
//!   the next batch.
//! * **Precompute workers** — read the handle directly in
//!   `get_or_create_group_state()`. No message passing, no polling;
//!   new agg_ids are visible the moment a worker tries to create a
//!   `GroupState` for them.
//!
//! ## Config-upgrade contract for the controller
//!
//! The recommended way for a controller to upgrade a metric's sketch
//! parameters (or aggregation type) is **monotonic, non-reused
//! `aggregation_id`s plus time-based retention**:
//!
//! 1. Controller decides to upgrade, e.g. `CMS(width=256)` →
//!    `CMS(width=1024)` for `test_metric`.
//! 2. Controller allocates a **new** `aggregation_id` (never reused),
//!    e.g. the old id was 1, the new id is 17.
//! 3. Controller POSTs a new `StreamingConfig` where the old id is
//!    **removed** and the new id is **added**:
//!    - before: `{1: CMS(width=256)}`
//!    - after:  `{17: CMS(width=1024)}`
//! 4. What happens on the backend, with zero additional code:
//!    - `IngestState` stops routing data to agg_id 1 and starts
//!      routing to agg_id 17 (metric-name match unchanged).
//!    - Workers evict the now-orphaned `GroupState` entries for
//!      agg_id 1 after their last windows drain
//!      (`evict_orphaned_groups`).
//!    - New `GroupState` entries for agg_id 17 are created on
//!      demand, with the new `CMS(width=1024)` parameters.
//!    - Store entries under agg_id 1 are **not deleted** on the
//!      config swap; they persist until `persistence_delete_older_than_secs`
//!      retention elapses, at which point the persistence layer's
//!      time-based TTL sweep drops the corresponding parts.
//! 5. Query semantics during the transition:
//!    - Before the swap: `ASAPQueryEngine` matches against agg_id 1.
//!    - After the swap: `ASAPQueryEngine` matches against agg_id 17.
//!      Historical data in the store under agg_id 1 is not joined
//!      into the answer; the new sketch warms up from zero.
//!    - Callers that need query continuity across parameter changes
//!      should implement an overlap period at the controller (keep
//!      both ids in the config long enough for the new id to accrue
//!      enough history) — this is a controller-side concern, not a
//!      backend one.
//!
//! ## What the contract requires from the controller
//!
//! * Assign `aggregation_id`s from a monotonically-increasing counter.
//! * Never reuse an `aggregation_id` after it has been removed from
//!   a `StreamingConfig` push.
//! * Rely on the backend's `persistence_delete_older_than_secs` for
//!   store cleanup — do not try to explicitly delete old agg_id data.
//!
//! Violating "never reuse" is safe in terms of correctness (the
//! backend creates a fresh `GroupState` either way), but it can
//! produce confusing store states where data under the same agg_id
//! spans multiple parameter generations.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::stores::schema::StreamingConfig;

/// Thin wrapper around `ArcSwap<StreamingConfig>` with ergonomic
/// snapshot + swap helpers. Cloneable; clones share the same
/// underlying `ArcSwap` so all holders see the same swaps.
#[derive(Clone)]
pub struct HotReloadStreamingConfig {
    inner: Arc<ArcSwap<StreamingConfig>>,
}

impl HotReloadStreamingConfig {
    /// Construct with an initial `StreamingConfig`. Takes ownership —
    /// callers who need to keep their own handle should `.clone()` the
    /// `StreamingConfig` before calling `new`.
    pub fn new(initial: StreamingConfig) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(initial))),
        }
    }

    /// Construct from a pre-built `Arc<StreamingConfig>` — useful
    /// when the caller already has the config behind an `Arc` and
    /// wants to avoid a redundant clone.
    pub fn from_arc(initial: Arc<StreamingConfig>) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
        }
    }

    /// Return a cheap, cloneable snapshot of the current config. The
    /// returned `Arc` is stable for the caller's lifetime — a
    /// concurrent swap produces a new `Arc` and leaves this one alone.
    pub fn snapshot(&self) -> Arc<StreamingConfig> {
        self.inner.load_full()
    }

    /// Atomically replace the current config. The previous `Arc` is
    /// dropped when the last reader holding it goes out of scope.
    /// Returns the `Arc` that was just replaced, for callers that
    /// want to diff old vs new (e.g. to log agg_ids that were added
    /// or removed).
    pub fn swap(&self, new: StreamingConfig) -> Arc<StreamingConfig> {
        self.inner.swap(Arc::new(new))
    }
}

impl std::fmt::Debug for HotReloadStreamingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot();
        f.debug_struct("HotReloadStreamingConfig")
            .field("num_agg_configs", &snap.aggregation_configs.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::schema::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::collections::HashMap;
    use std::thread;

    fn dummy_agg(id: u64) -> AggregationConfig {
        AggregationConfig::new(
            id,
            AggregationType::Sum,
            String::new(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            format!("metric_{id}"),
            None,
            None,
            None,
            None,
        )
    }

    fn cfg_with_ids(ids: &[u64]) -> StreamingConfig {
        let mut map = HashMap::new();
        for &id in ids {
            map.insert(id, dummy_agg(id));
        }
        StreamingConfig::new(map)
    }

    #[test]
    fn snapshot_reflects_initial_config() {
        let hr = HotReloadStreamingConfig::new(cfg_with_ids(&[1, 2, 3]));
        let snap = hr.snapshot();
        assert_eq!(snap.aggregation_configs.len(), 3);
        assert!(snap.aggregation_configs.contains_key(&2));
    }

    #[test]
    fn swap_replaces_config_atomically() {
        let hr = HotReloadStreamingConfig::new(cfg_with_ids(&[1, 2]));
        let old = hr.swap(cfg_with_ids(&[3, 4, 5]));
        // Old snapshot still reflects pre-swap contents.
        assert_eq!(old.aggregation_configs.len(), 2);
        assert!(old.aggregation_configs.contains_key(&1));
        // New snapshot reflects post-swap contents.
        let new_snap = hr.snapshot();
        assert_eq!(new_snap.aggregation_configs.len(), 3);
        assert!(new_snap.aggregation_configs.contains_key(&5));
        assert!(!new_snap.aggregation_configs.contains_key(&1));
    }

    #[test]
    fn clones_share_underlying_swap() {
        let hr = HotReloadStreamingConfig::new(cfg_with_ids(&[1]));
        let hr_clone = hr.clone();
        hr.swap(cfg_with_ids(&[2, 3]));
        // The clone sees the swap because both handles share the
        // same ArcSwap inside.
        let snap = hr_clone.snapshot();
        assert_eq!(snap.aggregation_configs.len(), 2);
        assert!(snap.aggregation_configs.contains_key(&3));
    }

    #[test]
    fn concurrent_readers_see_consistent_snapshot() {
        let hr = HotReloadStreamingConfig::new(cfg_with_ids(&[1, 2]));
        let hr_writer = hr.clone();
        let writer = thread::spawn(move || {
            for i in 0..50 {
                hr_writer.swap(cfg_with_ids(&[i, i + 1, i + 2]));
            }
        });
        let hr_reader = hr.clone();
        let reader = thread::spawn(move || {
            for _ in 0..200 {
                let snap = hr_reader.snapshot();
                // Under race, the snapshot must be internally
                // consistent — either 2 entries (original) or 3
                // (post-swap). Never a torn state.
                let n = snap.aggregation_configs.len();
                assert!(n == 2 || n == 3, "torn snapshot: {n} entries");
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
    }
}
