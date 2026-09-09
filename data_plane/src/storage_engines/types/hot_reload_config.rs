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

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::storage_engines::types::StreamingConfig;

/// One immutable, generation-consistent runtime snapshot. Every execution
/// subsystem must project its view from the same `Arc<ActivePhysicalPlan>`.
#[derive(Debug, Clone)]
pub struct ActivePhysicalPlan {
    pub precompute_plan: control_plane::physical::compiler::PrecomputePlan,
    pub transmission_plan: control_plane::physical::compiler::TransmissionPlan,
    pub runtime_config: Arc<StreamingConfig>,
    pub backend_plan: Arc<control_plane::backend_plan::BackendPlan>,
    pub query_plan: Arc<control_plane::query_plan::QueryPlan>,
    pub storage_routing: Arc<crate::storage_engines::types::BackendStorageRouting>,
}

#[derive(Clone)]
pub struct HotReloadActivePhysicalPlan {
    inner: Arc<ArcSwap<ActivePhysicalPlan>>,
    readiness: Arc<std::sync::Mutex<MaterializationReadinessState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationPhase {
    Materializing,
    Ready,
    Serving,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MaterializationStatus {
    pub plan_id: u64,
    pub plan_version: u64,
    pub materialization: u64,
    pub phase: MaterializationPhase,
    pub coverage_start_unix_ms: Option<u64>,
    pub coverage_end_unix_ms: Option<u64>,
}

struct MaterializationReadinessState {
    plan_id: u64,
    plan_version: u64,
    statuses: BTreeMap<asap_types::PolicyFingerprint, MaterializationStatus>,
}

impl MaterializationReadinessState {
    fn for_plan(plan: &ActivePhysicalPlan) -> Self {
        let plan_id = plan.backend_plan.plan_id;
        let plan_version = plan.backend_plan.plan_version;
        let statuses = plan
            .backend_plan
            .materializations
            .keys()
            .copied()
            .map(|materialization| {
                (
                    materialization,
                    MaterializationStatus {
                        plan_id,
                        plan_version,
                        materialization: materialization.0,
                        phase: MaterializationPhase::Materializing,
                        coverage_start_unix_ms: None,
                        coverage_end_unix_ms: None,
                    },
                )
            })
            .collect();
        Self {
            plan_id,
            plan_version,
            statuses,
        }
    }

    fn update(
        &mut self,
        plan_id: u64,
        plan_version: u64,
        materializations: &[asap_types::PolicyFingerprint],
        phase: MaterializationPhase,
        coverage: Option<(u64, u64)>,
    ) -> bool {
        if self.plan_id != plan_id || self.plan_version != plan_version {
            return false;
        }
        if materializations
            .iter()
            .any(|materialization| !self.statuses.contains_key(materialization))
        {
            return false;
        }
        for materialization in materializations {
            let status = self
                .statuses
                .get_mut(materialization)
                .expect("materializations were validated above");
            if phase != MaterializationPhase::Materializing
                || status.phase == MaterializationPhase::Materializing
            {
                status.phase = phase;
            }
            if let Some((start, end)) = coverage {
                status.coverage_start_unix_ms = Some(
                    status
                        .coverage_start_unix_ms
                        .map_or(start, |current| current.min(start)),
                );
                status.coverage_end_unix_ms = Some(
                    status
                        .coverage_end_unix_ms
                        .map_or(end, |current| current.max(end)),
                );
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalPlanPhase {
    Staged,
    Active,
    Draining,
    Retired,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PhysicalPlanStatus {
    pub plan_id: u64,
    pub plan_version: u64,
    pub phase: PhysicalPlanPhase,
    pub activation_unix_ms: u64,
    pub expiry_unix_ms: Option<u64>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PhysicalPlanLifecycleError {
    #[error("plan {plan_id}/{plan_version} is already staged or active")]
    Duplicate { plan_id: u64, plan_version: u64 },
    #[error("plan {plan_id}/{plan_version} is not staged")]
    NotStaged { plan_id: u64, plan_version: u64 },
    #[error("plan activation {activation} is later than now {now}")]
    ActivationNotReached { activation: u64, now: u64 },
    #[error("plan expired at {expiry}; now is {now}")]
    Expired { expiry: u64, now: u64 },
    #[error(
        "plan version {incoming} is not newer than active version {active} for plan {plan_id}"
    )]
    StaleVersion {
        plan_id: u64,
        incoming: u64,
        active: u64,
    },
}

#[derive(Clone)]
pub struct PhysicalPlanLifecycle {
    active: HotReloadActivePhysicalPlan,
    state: Arc<std::sync::Mutex<PhysicalPlanLifecycleState>>,
}

struct PhysicalPlanLifecycleState {
    staged: BTreeMap<(u64, u64), ActivePhysicalPlan>,
    statuses: BTreeMap<(u64, u64), PhysicalPlanStatus>,
}

impl PhysicalPlanLifecycle {
    pub fn new(active: HotReloadActivePhysicalPlan) -> Self {
        let snapshot = active.snapshot();
        let mut statuses = BTreeMap::new();
        if snapshot.backend_plan.plan_id != 0 {
            statuses.insert(
                (
                    snapshot.backend_plan.plan_id,
                    snapshot.backend_plan.plan_version,
                ),
                status_for(&snapshot, PhysicalPlanPhase::Active),
            );
        }
        Self {
            active,
            state: Arc::new(std::sync::Mutex::new(PhysicalPlanLifecycleState {
                staged: BTreeMap::new(),
                statuses,
            })),
        }
    }

    pub fn stage(
        &self,
        plan: ActivePhysicalPlan,
        now: u64,
    ) -> Result<(), PhysicalPlanLifecycleError> {
        let key = (plan.backend_plan.plan_id, plan.backend_plan.plan_version);
        if let Some(expiry) = plan.backend_plan.expiry_unix_ms {
            if expiry <= now {
                return Err(PhysicalPlanLifecycleError::Expired { expiry, now });
            }
        }
        let active = self.active.snapshot();
        if active.backend_plan.plan_id != 0 && key.1 <= active.backend_plan.plan_version {
            return Err(PhysicalPlanLifecycleError::StaleVersion {
                plan_id: key.0,
                incoming: key.1,
                active: active.backend_plan.plan_version,
            });
        }
        let mut state = self
            .state
            .lock()
            .expect("physical-plan lifecycle lock poisoned");
        if state.staged.contains_key(&key)
            || state.statuses.values().any(|status| {
                status.plan_version == key.1 && !matches!(status.phase, PhysicalPlanPhase::Retired)
            })
        {
            return Err(PhysicalPlanLifecycleError::Duplicate {
                plan_id: key.0,
                plan_version: key.1,
            });
        }
        state
            .statuses
            .insert(key, status_for(&plan, PhysicalPlanPhase::Staged));
        state.staged.insert(key, plan);
        Ok(())
    }

    /// Roll back a failed publication without touching active readers or state.
    pub fn discard_staged(
        &self,
        plan_id: u64,
        plan_version: u64,
    ) -> Result<(), PhysicalPlanLifecycleError> {
        let key = (plan_id, plan_version);
        let mut state = self
            .state
            .lock()
            .expect("physical-plan lifecycle lock poisoned");
        if state.staged.remove(&key).is_none() {
            return Err(PhysicalPlanLifecycleError::NotStaged {
                plan_id,
                plan_version,
            });
        }
        state.statuses.remove(&key);
        Ok(())
    }

    pub fn activate(
        &self,
        plan_id: u64,
        plan_version: u64,
        now: u64,
    ) -> Result<Arc<ActivePhysicalPlan>, PhysicalPlanLifecycleError> {
        let key = (plan_id, plan_version);
        let mut state = self
            .state
            .lock()
            .expect("physical-plan lifecycle lock poisoned");
        let plan = state
            .staged
            .get(&key)
            .ok_or(PhysicalPlanLifecycleError::NotStaged {
                plan_id,
                plan_version,
            })?;
        if now < plan.backend_plan.activation_unix_ms {
            return Err(PhysicalPlanLifecycleError::ActivationNotReached {
                activation: plan.backend_plan.activation_unix_ms,
                now,
            });
        }
        if let Some(expiry) = plan.backend_plan.expiry_unix_ms {
            if now >= expiry {
                return Err(PhysicalPlanLifecycleError::Expired { expiry, now });
            }
        }
        let current = self.active.snapshot();
        if current.backend_plan.plan_id != 0
            && plan.backend_plan.plan_version <= current.backend_plan.plan_version
        {
            return Err(PhysicalPlanLifecycleError::StaleVersion {
                plan_id,
                incoming: plan_version,
                active: current.backend_plan.plan_version,
            });
        }
        let plan = state.staged.remove(&key).expect("staged plan disappeared");
        let old = self.active.swap(plan.clone());
        if old.backend_plan.plan_id != 0 {
            if let Some(status) = state
                .statuses
                .get_mut(&(old.backend_plan.plan_id, old.backend_plan.plan_version))
            {
                status.phase = PhysicalPlanPhase::Draining;
            }
        }
        state
            .statuses
            .insert(key, status_for(&plan, PhysicalPlanPhase::Active));
        Ok(old)
    }

    pub fn statuses(&self) -> Vec<PhysicalPlanStatus> {
        self.state
            .lock()
            .expect("physical-plan lifecycle lock poisoned")
            .statuses
            .values()
            .cloned()
            .collect()
    }

    /// Mark a superseded generation retired after all readers of the old
    /// immutable snapshot have drained.
    pub fn retire_drained(&self, plan_id: u64, plan_version: u64) {
        if let Some(status) = self
            .state
            .lock()
            .expect("physical-plan lifecycle lock poisoned")
            .statuses
            .get_mut(&(plan_id, plan_version))
        {
            if status.phase == PhysicalPlanPhase::Draining {
                status.phase = PhysicalPlanPhase::Retired;
            }
        }
    }
}

fn status_for(plan: &ActivePhysicalPlan, phase: PhysicalPlanPhase) -> PhysicalPlanStatus {
    PhysicalPlanStatus {
        plan_id: plan.backend_plan.plan_id,
        plan_version: plan.backend_plan.plan_version,
        phase,
        activation_unix_ms: plan.backend_plan.activation_unix_ms,
        expiry_unix_ms: plan.backend_plan.expiry_unix_ms,
    }
}

impl HotReloadActivePhysicalPlan {
    pub fn new(initial: ActivePhysicalPlan) -> Self {
        let readiness = MaterializationReadinessState::for_plan(&initial);
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(initial))),
            readiness: Arc::new(std::sync::Mutex::new(readiness)),
        }
    }

    pub fn snapshot(&self) -> Arc<ActivePhysicalPlan> {
        self.inner.load_full()
    }

    pub fn swap(&self, next: ActivePhysicalPlan) -> Arc<ActivePhysicalPlan> {
        let next_readiness = MaterializationReadinessState::for_plan(&next);
        let old = self.inner.swap(Arc::new(next));
        *self
            .readiness
            .lock()
            .expect("materialization readiness lock poisoned") = next_readiness;
        old
    }

    pub fn materialization_statuses(&self) -> Vec<MaterializationStatus> {
        self.readiness
            .lock()
            .expect("materialization readiness lock poisoned")
            .statuses
            .values()
            .cloned()
            .collect()
    }

    pub fn mark_materializing(
        &self,
        plan_id: u64,
        plan_version: u64,
        materializations: &[asap_types::PolicyFingerprint],
        coverage: Option<(u64, u64)>,
    ) -> bool {
        self.update_materializations(
            plan_id,
            plan_version,
            materializations,
            MaterializationPhase::Materializing,
            coverage,
        )
    }

    pub fn mark_ready(
        &self,
        plan_id: u64,
        plan_version: u64,
        materializations: &[asap_types::PolicyFingerprint],
        coverage: (u64, u64),
    ) -> bool {
        self.update_materializations(
            plan_id,
            plan_version,
            materializations,
            MaterializationPhase::Ready,
            Some(coverage),
        )
    }

    pub fn mark_serving(
        &self,
        plan_id: u64,
        plan_version: u64,
        materializations: &[asap_types::PolicyFingerprint],
        coverage: (u64, u64),
    ) -> bool {
        self.update_materializations(
            plan_id,
            plan_version,
            materializations,
            MaterializationPhase::Serving,
            Some(coverage),
        )
    }

    fn update_materializations(
        &self,
        plan_id: u64,
        plan_version: u64,
        materializations: &[asap_types::PolicyFingerprint],
        phase: MaterializationPhase,
        coverage: Option<(u64, u64)>,
    ) -> bool {
        self.readiness
            .lock()
            .expect("materialization readiness lock poisoned")
            .update(plan_id, plan_version, materializations, phase, coverage)
    }
}

impl std::fmt::Debug for HotReloadActivePhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("HotReloadActivePhysicalPlan")
            .field("plan_id", &snapshot.backend_plan.plan_id)
            .field("query_count", &snapshot.query_plan.entries.len())
            .field(
                "materializations",
                &snapshot.precompute_plan.materializations.len(),
            )
            .finish()
    }
}

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
    active: Option<HotReloadActivePhysicalPlan>,
}

impl HotReloadBackendPlan {
    pub fn new(initial: control_plane::backend_plan::BackendPlan) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(initial))),
            install_lock: Arc::new(std::sync::Mutex::new(())),
            active: None,
        }
    }

    pub fn from_arc(initial: Arc<control_plane::backend_plan::BackendPlan>) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
            install_lock: Arc::new(std::sync::Mutex::new(())),
            active: None,
        }
    }

    pub fn from_active(active: HotReloadActivePhysicalPlan) -> Self {
        let initial = active.snapshot().backend_plan.clone();
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
            install_lock: Arc::new(std::sync::Mutex::new(())),
            active: Some(active),
        }
    }

    pub fn snapshot(&self) -> Arc<control_plane::backend_plan::BackendPlan> {
        self.active
            .as_ref()
            .map(|a| a.snapshot().backend_plan.clone())
            .unwrap_or_else(|| self.inner.load_full())
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
        if new.plan_id == active.plan_id && new.plan_id != 0 {
            if new.plan_version < active.plan_version {
                return Err(
                    control_plane::backend_plan::ValidationError::StalePlanVersion {
                        plan_id: new.plan_id,
                        incoming: new.plan_version,
                        active: active.plan_version,
                    },
                );
            }
            if new.plan_version == active.plan_version {
                if new == *active {
                    return Ok(active);
                }
                return Err(
                    control_plane::backend_plan::ValidationError::ReusedPlanVersion {
                        plan_id: new.plan_id,
                        plan_version: new.plan_version,
                    },
                );
            }
        }
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
            plan_version: 1,
            activation_unix_ms: 1,
            backend_compat: "asap-query-backend.v1".into(),
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
    active: Option<HotReloadActivePhysicalPlan>,
}

impl HotReloadStreamingConfig {
    /// Construct with an initial `StreamingConfig`. Takes ownership —
    /// callers who need to keep their own handle should `.clone()` the
    /// `StreamingConfig` before calling `new`.
    pub fn new(initial: StreamingConfig) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(initial))),
            active: None,
        }
    }

    /// Construct from a pre-built `Arc<StreamingConfig>` — useful
    /// when the caller already has the config behind an `Arc` and
    /// wants to avoid a redundant clone.
    pub fn from_arc(initial: Arc<StreamingConfig>) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
            active: None,
        }
    }

    pub fn from_active(active: HotReloadActivePhysicalPlan) -> Self {
        let initial = active.snapshot().runtime_config.clone();
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
            active: Some(active),
        }
    }

    /// Return a cheap, cloneable snapshot of the current config. The
    /// returned `Arc` is stable for the caller's lifetime — a
    /// concurrent swap produces a new `Arc` and leaves this one alone.
    pub fn snapshot(&self) -> Arc<StreamingConfig> {
        self.active
            .as_ref()
            .map(|a| a.snapshot().runtime_config.clone())
            .unwrap_or_else(|| self.inner.load_full())
    }

    pub fn physical_plan_snapshot(&self) -> Option<Arc<ActivePhysicalPlan>> {
        self.active.as_ref().map(|active| active.snapshot())
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

    fn physical_plan(
        plan_id: u64,
        plan_version: u64,
        activation_unix_ms: u64,
        expiry_unix_ms: Option<u64>,
    ) -> ActivePhysicalPlan {
        let envelope = control_plane::physical::compiler::PlanEnvelope {
            plan_id,
            plan_version,
            generated_at_unix_ms: activation_unix_ms,
            activation_unix_ms,
            expiry_unix_ms,
            backend_compat: "asap-query-backend.v1".into(),
            planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
            capability_snapshot_id: "test".into(),
        };
        ActivePhysicalPlan {
            precompute_plan: control_plane::physical::compiler::PrecomputePlan {
                summary_catalog: None,
                materialization_contracts: Default::default(),
                envelope,
                ingest: control_plane::physical::compiler::IngestContract {
                    protocol:
                        control_plane::physical::compiler::IngestProtocol::ModifiedOtlpMetricsV1,
                    endpoint_path: "/v1/metrics".into(),
                    timestamp_unit:
                        control_plane::physical::compiler::TimestampUnit::UnixNanoseconds,
                    require_plan_identity: true,
                    require_materialization_identity: true,
                    require_registered_producer: true,
                },
                schemas: Vec::new(),
                producers: Vec::new(),
                materializations: Vec::new(),
            },
            transmission_plan: control_plane::physical::compiler::TransmissionPlan {
                summary_catalog: None,
                envelope: control_plane::physical::compiler::PlanEnvelope {
                    plan_id,
                    plan_version,
                    generated_at_unix_ms: activation_unix_ms,
                    activation_unix_ms,
                    expiry_unix_ms,
                    backend_compat: "asap-query-backend.v1".into(),
                    planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
                    capability_snapshot_id: "test".into(),
                },
                frame_identity: control_plane::physical::compiler::FrameIdentityContract {
                    identity_version: 1,
                    sequence_scope: control_plane::physical::compiler::SequenceScope::MaterializationSeriesProducerEpoch,
                    require_checkpoint_for_full: true,
                    require_base_checkpoint_for_delta: true,
                },
                rules: Vec::new(),
            },
            runtime_config: Arc::new(StreamingConfig::new(HashMap::new())),
            backend_plan: Arc::new(control_plane::backend_plan::BackendPlan {
                plan_id,
                plan_version,
                generated_at_unix_ms: activation_unix_ms,
                activation_unix_ms,
                expiry_unix_ms,
                backend_compat: "asap-query-backend.v1".into(),
                ..Default::default()
            }),
            query_plan: Arc::new(control_plane::query_plan::QueryPlan {
                plan_id,
                plan_version,
                entries: Default::default(),
            }),
            storage_routing: Arc::new(crate::storage_engines::types::BackendStorageRouting::empty()),
        }
    }

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

    #[test]
    fn physical_plan_stages_activates_drains_and_retires() {
        let active = HotReloadActivePhysicalPlan::new(physical_plan(7, 1, 100, None));
        let lifecycle = PhysicalPlanLifecycle::new(active.clone());
        lifecycle
            .stage(physical_plan(7, 2, 200, Some(500)), 150)
            .unwrap();

        assert_eq!(active.snapshot().backend_plan.plan_version, 1);
        assert!(matches!(
            lifecycle.activate(7, 2, 199),
            Err(PhysicalPlanLifecycleError::ActivationNotReached { .. })
        ));
        let old = lifecycle.activate(7, 2, 200).unwrap();
        assert_eq!(old.backend_plan.plan_version, 1);
        assert_eq!(active.snapshot().backend_plan.plan_version, 2);

        let statuses = lifecycle.statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].phase, PhysicalPlanPhase::Draining);
        assert_eq!(statuses[1].phase, PhysicalPlanPhase::Active);
        lifecycle.retire_drained(7, 1);
        assert_eq!(lifecycle.statuses()[0].phase, PhysicalPlanPhase::Retired);
    }

    // Failed publication releases only its staging slot; active readers remain valid.
    #[test]
    fn discard_staged_allows_retry_and_never_discards_active() {
        let active = HotReloadActivePhysicalPlan::new(physical_plan(7, 1, 100, None));
        let held_reader = active.snapshot();
        let lifecycle = PhysicalPlanLifecycle::new(active.clone());
        lifecycle
            .stage(physical_plan(7, 2, 200, None), 150)
            .unwrap();
        lifecycle.discard_staged(7, 2).unwrap();
        lifecycle
            .stage(physical_plan(7, 2, 300, None), 250)
            .unwrap();
        lifecycle.activate(7, 2, 300).unwrap();
        assert!(lifecycle.discard_staged(7, 2).is_err());
        assert_eq!(active.snapshot().backend_plan.plan_version, 2);
        assert_eq!(held_reader.backend_plan.plan_version, 1);
    }

    #[test]
    fn materialization_readiness_is_generation_scoped_and_monotonic() {
        let active = HotReloadActivePhysicalPlan::new(physical_plan(7, 1, 100, None));
        let fingerprint = asap_types::PolicyFingerprint(41);
        active.readiness.lock().unwrap().statuses.insert(
            fingerprint,
            MaterializationStatus {
                plan_id: 7,
                plan_version: 1,
                materialization: fingerprint.0,
                phase: MaterializationPhase::Materializing,
                coverage_start_unix_ms: None,
                coverage_end_unix_ms: None,
            },
        );

        assert!(active.mark_ready(7, 1, &[fingerprint], (100, 200)));
        assert!(active.mark_serving(7, 1, &[fingerprint], (100, 300)));
        assert!(active.mark_materializing(7, 1, &[fingerprint], Some((200, 250))));
        let status = active.materialization_statuses().pop().unwrap();
        assert_eq!(status.phase, MaterializationPhase::Serving);
        assert_eq!(status.coverage_start_unix_ms, Some(100));
        assert_eq!(status.coverage_end_unix_ms, Some(300));

        assert!(!active.mark_ready(7, 2, &[fingerprint], (100, 400)));
        assert_eq!(
            active.materialization_statuses()[0].coverage_end_unix_ms,
            Some(300)
        );
    }

    #[test]
    fn physical_plan_rejects_stale_and_expired_generations() {
        let active = HotReloadActivePhysicalPlan::new(physical_plan(7, 2, 100, None));
        let lifecycle = PhysicalPlanLifecycle::new(active);
        assert!(matches!(
            lifecycle.stage(physical_plan(7, 1, 100, None), 150),
            Err(PhysicalPlanLifecycleError::StaleVersion { .. })
        ));
        assert!(matches!(
            lifecycle.stage(physical_plan(8, 1, 100, Some(150)), 150),
            Err(PhysicalPlanLifecycleError::Expired { .. })
        ));
    }

    #[test]
    fn activation_rechecks_version_and_cannot_downgrade_across_plan_ids() {
        let active = HotReloadActivePhysicalPlan::new(physical_plan(7, 1, 100, None));
        let lifecycle = PhysicalPlanLifecycle::new(active.clone());
        lifecycle
            .stage(physical_plan(8, 3, 100, None), 100)
            .unwrap();
        lifecycle
            .stage(physical_plan(9, 2, 100, None), 100)
            .unwrap();
        lifecycle.activate(8, 3, 100).unwrap();

        assert!(matches!(
            lifecycle.activate(9, 2, 100),
            Err(PhysicalPlanLifecycleError::StaleVersion {
                incoming: 2,
                active: 3,
                ..
            })
        ));
        assert_eq!(active.snapshot().backend_plan.plan_version, 3);
    }
}
