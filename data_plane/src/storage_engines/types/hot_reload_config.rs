//! Hot-reloadable `StreamingConfig` state.
//!
//! Wraps a shared `StreamingConfig` in `arc_swap::ArcSwap` so an
//! external control plane can push a new config at runtime via
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
//! ## Config-upgrade contract for the control plane
//!
//! The recommended way for a control plane to upgrade a metric's sketch
//! parameters (or aggregation type) is **monotonic, non-reused
//! `aggregation_id`s plus time-based retention**:
//!
//! 1. Control plane decides to upgrade, e.g. `CMS(width=256)` →
//!    `CMS(width=1024)` for `test_metric`.
//! 2. Control plane allocates a **new** `aggregation_id` (never reused),
//!    e.g. the old id was 1, the new id is 17.
//! 3. Control plane POSTs a new `StreamingConfig` where the old id is
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
//!      should implement an overlap period at the control plane (keep
//!      both ids in the config long enough for the new id to accrue
//!      enough history) — this is a control-plane-side concern, not a
//!      backend one.
//!
//! ## What the contract requires from the control plane
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

use crate::storage_engines::types::StreamingConfig;

/// Hot-reloadable `BackendPlan` state — same `ArcSwap` shape as
/// [`HotReloadStreamingConfig`], applied to
/// `control_plane::backend_plan::BackendPlan` (see
/// `control_plane/docs/design-backend-plan-wire-format.md`). Lives
/// alongside [`HotReloadStreamingConfig`], not in place of it:
/// `POST /api/v1/backend-plan` installs the latest plan here for
/// `ASAPQueryEngine`'s serving-time lookup to read, while
/// `POST /api/v1/streaming-config` still drives sid-catalog lifecycle
/// (registration/retirement) on its own path.
#[derive(Clone)]
pub struct HotReloadBackendPlan {
    inner: Arc<ArcSwap<control_plane::backend_plan::BackendPlan>>,
    install_lock: Arc<std::sync::Mutex<()>>,
}

impl HotReloadBackendPlan {
    pub fn new(initial: control_plane::backend_plan::BackendPlan) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(initial))),
            install_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }

    pub fn from_arc(initial: Arc<control_plane::backend_plan::BackendPlan>) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
            install_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }

    pub fn snapshot(&self) -> Arc<control_plane::backend_plan::BackendPlan> {
        self.inner.load_full()
    }

    pub fn swap(
        &self,
        new: control_plane::backend_plan::BackendPlan,
    ) -> Arc<control_plane::backend_plan::BackendPlan> {
        self.inner.swap(Arc::new(new))
    }

    /// Validate then atomically install a plan. A rejected plan never becomes
    /// observable and the previous snapshot remains active.
    pub fn install(
        &self,
        new: control_plane::backend_plan::BackendPlan,
    ) -> Result<
        Arc<control_plane::backend_plan::BackendPlan>,
        control_plane::backend_plan::ValidationError,
    > {
        let _guard = self
            .install_lock
            .lock()
            .expect("backend-plan install lock poisoned");
        new.validate()?;
        let active = self.snapshot();
        if new.generated_at_unix_ms < active.generated_at_unix_ms {
            return Err(
                control_plane::backend_plan::ValidationError::StaleGeneration {
                    incoming: new.generated_at_unix_ms,
                    active: active.generated_at_unix_ms,
                },
            );
        }
        Ok(self.swap(new))
    }
}

impl Default for HotReloadBackendPlan {
    fn default() -> Self {
        Self::new(control_plane::backend_plan::BackendPlan::default())
    }
}

impl std::fmt::Debug for HotReloadBackendPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot();
        f.debug_struct("HotReloadBackendPlan")
            .field("plan_id", &snap.plan_id)
            .field("materializations", &snap.materializations.len())
            .field("routing", &snap.routing.len())
            .finish()
    }
}

#[cfg(test)]
mod hot_reload_backend_plan_tests {
    use super::*;
    use control_plane::backend_plan::BackendPlan;

    fn plan(plan_id: u64) -> BackendPlan {
        BackendPlan {
            plan_id,
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_reflects_initial_plan() {
        let hr = HotReloadBackendPlan::new(plan(1));
        assert_eq!(hr.snapshot().plan_id, 1);
    }

    #[test]
    fn swap_replaces_plan_atomically() {
        let hr = HotReloadBackendPlan::new(plan(1));
        let old = hr.swap(plan(2));
        assert_eq!(old.plan_id, 1, "swap returns the pre-swap snapshot");
        assert_eq!(hr.snapshot().plan_id, 2);
    }

    #[test]
    fn clones_share_underlying_swap() {
        let hr = HotReloadBackendPlan::new(plan(1));
        let hr_clone = hr.clone();
        hr.swap(plan(2));
        assert_eq!(hr_clone.snapshot().plan_id, 2);
    }

    #[test]
    fn invalid_plan_is_rejected_without_replacing_snapshot() {
        let hr = HotReloadBackendPlan::new(plan(1));
        let mut invalid = plan(2);
        invalid
            .routing
            .push(control_plane::backend_plan::RoutingEntry {
                satisfies: control_plane::physical::runtime_capability::Capability::ExactAgg(
                    asap_types::AggregationType::Sum,
                ),
                materialization: asap_types::PolicyFingerprint(99),
                storage_backend: control_plane::backend_plan::StorageBackend::SketchStore,
            });
        assert!(hr.install(invalid).is_err());
        assert_eq!(hr.snapshot().plan_id, 1);
    }

    #[test]
    fn older_generation_is_rejected_without_replacing_snapshot() {
        let mut current = plan(2);
        current.generated_at_unix_ms = 200;
        let hr = HotReloadBackendPlan::new(current);
        let mut stale = plan(3);
        stale.generated_at_unix_ms = 199;
        assert!(matches!(
            hr.install(stale),
            Err(control_plane::backend_plan::ValidationError::StaleGeneration { .. })
        ));
        assert_eq!(hr.snapshot().plan_id, 2);
    }
}

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
    use crate::storage_engines::types::AggregationConfig;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::collections::HashMap;
    use std::thread;

    fn dummy_agg(id: u64) -> AggregationConfig {
        AggregationConfig::new(
            AggregationType::Sum,
            String::new(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            format!("metric_{id}"),
            None,
            None,
            None,
        )
    }

    /// Build a StreamingConfig from a list of marker `id`s. After PR 5
    /// the map key IS the policy fingerprint, derived from
    /// `metric_{id}`. We return both the config and the
    /// dummy-id→fingerprint mapping so the assertions below can look
    /// up entries.
    fn cfg_with_ids(ids: &[u64]) -> (StreamingConfig, std::collections::HashMap<u64, u64>) {
        let mut map = HashMap::new();
        let mut id_to_fp = std::collections::HashMap::new();
        for &id in ids {
            let cfg = dummy_agg(id);
            let fp = cfg.policy_fp_u64();
            id_to_fp.insert(id, fp);
            map.insert(fp, cfg);
        }
        (StreamingConfig::new(map), id_to_fp)
    }

    #[test]
    fn snapshot_reflects_initial_config() {
        let (cfg, id_to_fp) = cfg_with_ids(&[1, 2, 3]);
        let hr = HotReloadStreamingConfig::new(cfg);
        let snap = hr.snapshot();
        assert_eq!(snap.aggregation_configs.len(), 3);
        assert!(snap.aggregation_configs.contains_key(&id_to_fp[&2]));
    }

    #[test]
    fn swap_replaces_config_atomically() {
        let (cfg1, id_to_fp1) = cfg_with_ids(&[1, 2]);
        let (cfg2, id_to_fp2) = cfg_with_ids(&[3, 4, 5]);
        let hr = HotReloadStreamingConfig::new(cfg1);
        let old = hr.swap(cfg2);
        // Old snapshot still reflects pre-swap contents.
        assert_eq!(old.aggregation_configs.len(), 2);
        assert!(old.aggregation_configs.contains_key(&id_to_fp1[&1]));
        // New snapshot reflects post-swap contents.
        let new_snap = hr.snapshot();
        assert_eq!(new_snap.aggregation_configs.len(), 3);
        assert!(new_snap.aggregation_configs.contains_key(&id_to_fp2[&5]));
        assert!(!new_snap.aggregation_configs.contains_key(&id_to_fp1[&1]));
    }

    #[test]
    fn clones_share_underlying_swap() {
        let (cfg1, _) = cfg_with_ids(&[1]);
        let (cfg2, id_to_fp2) = cfg_with_ids(&[2, 3]);
        let hr = HotReloadStreamingConfig::new(cfg1);
        let hr_clone = hr.clone();
        hr.swap(cfg2);
        // The clone sees the swap because both handles share the
        // same ArcSwap inside.
        let snap = hr_clone.snapshot();
        assert_eq!(snap.aggregation_configs.len(), 2);
        assert!(snap.aggregation_configs.contains_key(&id_to_fp2[&3]));
    }

    #[test]
    fn concurrent_readers_see_consistent_snapshot() {
        let (cfg1, _) = cfg_with_ids(&[1, 2]);
        let hr = HotReloadStreamingConfig::new(cfg1);
        let hr_writer = hr.clone();
        let writer = thread::spawn(move || {
            for i in 0..50 {
                let (c, _) = cfg_with_ids(&[i, i + 1, i + 2]);
                hr_writer.swap(c);
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
