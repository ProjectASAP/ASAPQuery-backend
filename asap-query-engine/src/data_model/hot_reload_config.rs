//! Hot-reloadable `StreamingConfig` state.
//!
//! Wraps a shared `StreamingConfig` in `arc_swap::ArcSwap` so an
//! external controller (or a test harness) can push a new config at
//! runtime via `POST /api/v1/streaming-config` without restarting the
//! query engine binary. Phase 1 of the StreamingConfig hot-reload
//! effort (ASAPQuery PR E).
//!
//! ## Contract (phase 1)
//!
//! * **Writes** — atomic via `ArcSwap::store`. The write side is
//!   lock-free; readers that hold a stale snapshot finish their work
//!   with the old config and drop it when the last reference goes
//!   out of scope (standard `Arc` refcounting).
//! * **Reads for query execution** — `SimpleEngine` takes a long-lived
//!   startup snapshot today and does not yet re-snapshot per query.
//!   That is tracked as a **phase 2** follow-up; see the "What's NOT
//!   hot-reloaded yet" section of the PR description.
//! * **Reads for the control plane** — the `GET /api/v1/streaming-config`
//!   debug endpoint always reflects the latest swapped config, so
//!   integration tests and operators can verify a push landed.
//! * **In-flight worker state** — precompute workers hold per-`(agg_id,
//!   group_key)` `GroupState` objects whose `Arc<AggregationConfig>`
//!   was cloned at group creation time. Those in-flight windows
//!   continue with their construction-time config and flush normally;
//!   new groups created after the swap pick up the new config. This
//!   yields correct semantics for the common controller use case
//!   (adding a new agg_id, or adjusting parameters that only take
//!   effect on the next window) without draining open windows.
//!
//! Removing an `agg_id` mid-window is the one case where phase 1 is
//! visibly incomplete — existing groups for that id continue ingesting
//! until they close naturally. The `POST` handler logs a warning when
//! a swap removes agg_ids that currently have live state.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::data_model::StreamingConfig;

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
    use crate::data_model::AggregationConfig;
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
