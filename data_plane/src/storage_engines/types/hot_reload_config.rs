//! Generation-consistent physical plan installation and execution views.
//!
//! Production consumers obtain the installed precompute program through the
//! active physical plan. Plan publication installs query and precompute bindings
//! together; a runtime lookup view cannot independently redefine computation.

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::storage_engines::types::InstalledPrecomputePlan;

/// One immutable, generation-consistent runtime snapshot. Every execution
/// subsystem must project its view from the same `Arc<RuntimePhysicalPlan>`.
#[derive(Debug, Clone)]
pub struct RuntimePhysicalPlan {
    /// Authoritative generation and lifecycle identity shared by every plan
    /// projection in this immutable snapshot.
    pub envelope: asap_types::precompute_plan::PlanEnvelope,
    /// Present for authoritative installations; legacy bootstrap has no catalog.
    pub summary_catalog: Option<Arc<control_plane::physical::summary_catalog::SummaryCatalog>>,
    pub precompute_plan: asap_types::precompute_plan::PrecomputePlan,
    pub transmission_plan: asap_types::producer_plan::TransmissionPlan,
    pub installed_precompute_plan: Arc<InstalledPrecomputePlan>,
    pub query_plan: Arc<asap_types::query_plan::QueryPlan>,
    /// Immutable readout-boundary programs for this snapshot. This holds plan
    /// metadata only; data, coverage and readiness are resolved on every run.
    pub readout_programs: Arc<PreparedReadoutPrograms>,
    pub storage_routing: Arc<crate::storage_engines::types::BackendStorageRouting>,
}

type ReadoutRoots =
    BTreeMap<asap_types::query_plan::QueryNodeId, Arc<asap_types::query_plan::QueryPlanEntry>>;

#[derive(Debug, Default)]
struct ReadoutProgramRegistry {
    query_plan: Option<Arc<asap_types::query_plan::QueryPlan>>,
    entries: std::collections::HashMap<asap_types::QueryLanguage, BTreeMap<String, ReadoutRoots>>,
}

#[derive(Debug, Default)]
pub struct PreparedReadoutPrograms {
    registry: std::sync::Mutex<ReadoutProgramRegistry>,
}

impl RuntimePhysicalPlan {
    /// Prepare an already-selected readout boundary once for this immutable
    /// deployment snapshot. No rows, state, results or readiness are retained.
    pub(crate) fn readout_program(
        &self,
        entry: &asap_types::query_plan::QueryPlanEntry,
        root: asap_types::query_plan::QueryNodeId,
    ) -> Result<Arc<asap_types::query_plan::QueryPlanEntry>, String> {
        let mut programs = self
            .readout_programs
            .registry
            .lock()
            .map_err(|_| "readout program registry poisoned".to_string())?;
        // Cloning a snapshot and replacing its QueryPlan must not preserve
        // bindings from the previous immutable plan, even with identical names.
        if programs
            .query_plan
            .as_ref()
            .is_none_or(|installed| !Arc::ptr_eq(installed, &self.query_plan))
        {
            programs.entries.clear();
            programs.query_plan = Some(Arc::clone(&self.query_plan));
        }
        if let Some(program) = programs
            .entries
            .get(&entry.language)
            .and_then(|queries| queries.get(&entry.canonical_query))
            .and_then(|roots| roots.get(&root))
        {
            return Ok(Arc::clone(program));
        }
        let reachable = entry
            .topological_order_from(root)
            .map_err(|error| error.to_string())?;
        let mut program = entry.clone();
        program.root = root;
        program.nodes = reachable
            .into_iter()
            .map(|id| (id, entry.nodes[&id].clone()))
            .collect();
        let windows: std::collections::BTreeSet<_> = program
            .materialization_bindings()
            .iter()
            .map(|binding| binding.readout_lookback_ms)
            .collect();
        if windows.len() != 1 || windows.contains(&None) || windows.contains(&Some(0)) {
            return Err("bound subtree requires one explicit positive window".into());
        }
        program.instant.lookback_ms = windows
            .first()
            .copied()
            .flatten()
            .expect("explicit semantic lookback checked");
        program.instant.full_history = false;
        program.instant.cumulative_readout = true;
        let program = Arc::new(program);
        programs
            .entries
            .entry(entry.language)
            .or_default()
            .entry(entry.canonical_query.clone())
            .or_default()
            .insert(root, Arc::clone(&program));
        Ok(program)
    }

    pub fn plan_id(&self) -> u64 {
        self.envelope.plan_id
    }
    pub fn plan_version(&self) -> u64 {
        self.envelope.plan_version
    }
    pub fn activation_unix_ms(&self) -> u64 {
        self.envelope.activation_unix_ms
    }
    pub fn expiry_unix_ms(&self) -> Option<u64> {
        self.envelope.expiry_unix_ms
    }

    fn materialization_fingerprints(
        &self,
    ) -> impl Iterator<Item = asap_types::PolicyFingerprint> + '_ {
        self.precompute_plan
            .materializations
            .iter()
            .map(asap_types::PrecomputeMaterialization::policy_fingerprint)
    }
}

#[derive(Clone)]
pub struct ActivePhysicalPlanHandle {
    inner: Arc<ArcSwap<RuntimePhysicalPlan>>,
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
    fn for_plan(plan: &RuntimePhysicalPlan) -> Self {
        let plan_id = plan.plan_id();
        let plan_version = plan.plan_version();
        let statuses = plan
            .materialization_fingerprints()
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
    #[error("plan activation preparation failed: {0}")]
    Prepare(String),
}

#[derive(Clone)]
pub struct PhysicalPlanLifecycle {
    active: ActivePhysicalPlanHandle,
    state: Arc<std::sync::Mutex<PhysicalPlanLifecycleState>>,
}

struct PhysicalPlanLifecycleState {
    staged: BTreeMap<(u64, u64), RuntimePhysicalPlan>,
    statuses: BTreeMap<(u64, u64), PhysicalPlanStatus>,
}

impl PhysicalPlanLifecycle {
    pub fn new(active: ActivePhysicalPlanHandle) -> Self {
        let snapshot = active.active_snapshot();
        let mut statuses = BTreeMap::new();
        if snapshot.plan_id() != 0 {
            statuses.insert(
                (snapshot.plan_id(), snapshot.plan_version()),
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
        plan: RuntimePhysicalPlan,
        now: u64,
    ) -> Result<(), PhysicalPlanLifecycleError> {
        let key = (plan.plan_id(), plan.plan_version());
        if let Some(expiry) = plan.expiry_unix_ms() {
            if expiry <= now {
                return Err(PhysicalPlanLifecycleError::Expired { expiry, now });
            }
        }
        let active = self.active.active_snapshot();
        if active.plan_id() != 0 && key.1 <= active.plan_version() {
            return Err(PhysicalPlanLifecycleError::StaleVersion {
                plan_id: key.0,
                incoming: key.1,
                active: active.plan_version(),
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
    pub(crate) fn discard_staged(
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
    ) -> Result<Arc<RuntimePhysicalPlan>, PhysicalPlanLifecycleError> {
        self.activate_with_prepare(plan_id, plan_version, now, |_| Ok::<(), String>(()))
    }

    /// Run generation-scoped preparation before publishing a staged plan.
    /// A failed preparation leaves both the active and staged generations
    /// unchanged, so callers can fix the dependency and retry activation.
    pub fn activate_with_prepare<E>(
        &self,
        plan_id: u64,
        plan_version: u64,
        now: u64,
        prepare: impl FnOnce(&RuntimePhysicalPlan) -> Result<(), E>,
    ) -> Result<Arc<RuntimePhysicalPlan>, PhysicalPlanLifecycleError>
    where
        E: std::fmt::Display,
    {
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
        if now < plan.activation_unix_ms() {
            return Err(PhysicalPlanLifecycleError::ActivationNotReached {
                activation: plan.activation_unix_ms(),
                now,
            });
        }
        if let Some(expiry) = plan.expiry_unix_ms() {
            if now >= expiry {
                return Err(PhysicalPlanLifecycleError::Expired { expiry, now });
            }
        }
        let current = self.active.active_snapshot();
        if current.plan_id() != 0 && plan.plan_version() <= current.plan_version() {
            return Err(PhysicalPlanLifecycleError::StaleVersion {
                plan_id,
                incoming: plan_version,
                active: current.plan_version(),
            });
        }
        prepare(plan).map_err(|error| PhysicalPlanLifecycleError::Prepare(error.to_string()))?;
        let plan = state.staged.remove(&key).expect("staged plan disappeared");
        let old = self.active.swap(plan.clone());
        if old.plan_id() != 0 {
            if let Some(status) = state.statuses.get_mut(&(old.plan_id(), old.plan_version())) {
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
    pub(crate) fn mark_drained_plan_retired(&self, plan_id: u64, plan_version: u64) {
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

fn status_for(plan: &RuntimePhysicalPlan, phase: PhysicalPlanPhase) -> PhysicalPlanStatus {
    PhysicalPlanStatus {
        plan_id: plan.plan_id(),
        plan_version: plan.plan_version(),
        phase,
        activation_unix_ms: plan.activation_unix_ms(),
        expiry_unix_ms: plan.expiry_unix_ms(),
    }
}

impl ActivePhysicalPlanHandle {
    pub fn new(initial: RuntimePhysicalPlan) -> Self {
        let readiness = MaterializationReadinessState::for_plan(&initial);
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(initial))),
            readiness: Arc::new(std::sync::Mutex::new(readiness)),
        }
    }

    pub fn active_snapshot(&self) -> Arc<RuntimePhysicalPlan> {
        self.inner.load_full()
    }

    pub fn swap(&self, next: RuntimePhysicalPlan) -> Arc<RuntimePhysicalPlan> {
        let next_readiness = MaterializationReadinessState::for_plan(&next);
        let old = self.inner.swap(Arc::new(next));
        *self
            .readiness
            .lock()
            .expect("materialization readiness lock poisoned") = next_readiness;
        old
    }

    pub(crate) fn materialization_statuses(&self) -> Vec<MaterializationStatus> {
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

impl std::fmt::Debug for ActivePhysicalPlanHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.active_snapshot();
        f.debug_struct("ActivePhysicalPlanHandle")
            .field("plan_id", &snapshot.plan_id())
            .field("query_count", &snapshot.query_plan.entries.len())
            .field(
                "materializations",
                &snapshot.precompute_plan.materializations.len(),
            )
            .finish()
    }
}

/// Precompute projection of the authoritative active physical plan.
#[derive(Clone)]
pub struct InstalledPrecomputePlanHandle {
    source: InstalledPrecomputePlanSource,
}

#[derive(Clone)]
enum InstalledPrecomputePlanSource {
    Active(ActivePhysicalPlanHandle),
    #[cfg(test)]
    Fixture(Arc<ArcSwap<InstalledPrecomputePlan>>),
}

impl InstalledPrecomputePlanHandle {
    #[cfg(test)]
    pub fn new(initial: InstalledPrecomputePlan) -> Self {
        Self::from_arc(Arc::new(initial))
    }

    #[cfg(test)]
    pub fn from_arc(initial: Arc<InstalledPrecomputePlan>) -> Self {
        Self {
            source: InstalledPrecomputePlanSource::Fixture(Arc::new(ArcSwap::new(initial))),
        }
    }

    pub fn from_active_physical_plan(active: ActivePhysicalPlanHandle) -> Self {
        Self {
            source: InstalledPrecomputePlanSource::Active(active),
        }
    }

    pub fn snapshot(&self) -> Arc<InstalledPrecomputePlan> {
        match &self.source {
            InstalledPrecomputePlanSource::Active(active) => {
                active.active_snapshot().installed_precompute_plan.clone()
            }
            #[cfg(test)]
            InstalledPrecomputePlanSource::Fixture(view) => view.load_full(),
        }
    }

    pub fn active_physical_plan_snapshot(&self) -> Option<Arc<RuntimePhysicalPlan>> {
        match &self.source {
            InstalledPrecomputePlanSource::Active(active) => Some(active.active_snapshot()),
            #[cfg(test)]
            InstalledPrecomputePlanSource::Fixture(_) => None,
        }
    }

    #[cfg(test)]
    pub fn swap(&self, new: InstalledPrecomputePlan) -> Arc<InstalledPrecomputePlan> {
        match &self.source {
            InstalledPrecomputePlanSource::Fixture(view) => view.swap(Arc::new(new)),
            InstalledPrecomputePlanSource::Active(_) => panic!("activate a complete physical plan"),
        }
    }
}

impl std::fmt::Debug for InstalledPrecomputePlanHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot();
        f.debug_struct("InstalledPrecomputePlanHandle")
            .field(
                "num_agg_configs",
                &snap.materializations_by_policy_fingerprint.len(),
            )
            .finish()
    }
}

// Compatibility imports; new callers use the domain names above.
#[deprecated(note = "Use ActivePhysicalPlanHandle")]
pub use ActivePhysicalPlanHandle as HotReloadActivePhysicalPlan;
#[deprecated(note = "Use RuntimePhysicalPlan")]
pub use RuntimePhysicalPlan as ActivePhysicalPlan;

impl InstalledPrecomputePlanHandle {
    #[deprecated(note = "Use from_active_physical_plan")]
    pub fn from_active(active: ActivePhysicalPlanHandle) -> Self {
        Self::from_active_physical_plan(active)
    }
    #[deprecated(note = "Use active_physical_plan_snapshot")]
    pub fn physical_plan_snapshot(&self) -> Option<Arc<RuntimePhysicalPlan>> {
        self.active_physical_plan_snapshot()
    }
}
impl PhysicalPlanLifecycle {
    #[deprecated(note = "Use mark_drained_plan_retired")]
    pub fn retire_drained(&self, plan_id: u64, plan_version: u64) {
        self.mark_drained_plan_retired(plan_id, plan_version)
    }
}

impl ActivePhysicalPlanHandle {
    #[deprecated(note = "Use active_snapshot")]
    pub fn snapshot(&self) -> Arc<RuntimePhysicalPlan> {
        self.active_snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::types::PrecomputeMaterialization;
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
    ) -> RuntimePhysicalPlan {
        let envelope = asap_types::precompute_plan::PlanEnvelope {
            plan_id,
            plan_version,
            generated_at_unix_ms: activation_unix_ms,
            activation_unix_ms,
            expiry_unix_ms,
            backend_compat: "asap-query-backend.v1".into(),
            planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
            capability_snapshot_id: "test".into(),
        };
        RuntimePhysicalPlan {
            readout_programs: Default::default(),
            envelope: envelope.clone(),
            summary_catalog: None,
            precompute_plan: asap_types::precompute_plan::PrecomputePlan {
                summary_catalog: None,
                envelope: envelope.clone(),
                ingest: asap_types::precompute_plan::IngestContract {
                    protocol: asap_types::precompute_plan::IngestProtocol::ModifiedOtlpMetricsV1,
                    endpoint_path: "/v1/metrics".into(),
                    timestamp_unit: asap_types::precompute_plan::TimestampUnit::UnixNanoseconds,
                    require_plan_identity: true,
                    require_summary_definition_identity: true,
                    require_registered_producer: true,
                },
                schemas: Vec::new(),
                producers: Vec::new(),
                executable_dags: Default::default(),
                materializations: Vec::new(),
            },
            transmission_plan: asap_types::producer_plan::TransmissionPlan {
                summary_catalog: None,
                envelope: asap_types::precompute_plan::PlanEnvelope {
                    plan_id,
                    plan_version,
                    generated_at_unix_ms: activation_unix_ms,
                    activation_unix_ms,
                    expiry_unix_ms,
                    backend_compat: "asap-query-backend.v1".into(),
                    planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
                    capability_snapshot_id: "test".into(),
                },
                frame_identity: asap_types::producer_plan::FrameIdentityContract {
                    identity_version: 1,
                    sequence_scope:
                        asap_types::producer_plan::SequenceScope::MaterializationSeriesProducerEpoch,
                    require_checkpoint_for_full: true,
                    require_base_checkpoint_for_delta: true,
                },
                rules: Vec::new(),
            },
            installed_precompute_plan: Arc::new(InstalledPrecomputePlan::new(HashMap::new())),
            query_plan: Arc::new(asap_types::query_plan::QueryPlan {
                plan_id,
                plan_version,
                clickhouse_context: None,
                selected_dags: Default::default(),
                entries: Default::default(),
            }),
            storage_routing: Arc::new(crate::storage_engines::types::BackendStorageRouting::empty()),
        }
    }

    #[test]
    fn readout_programs_follow_replaced_query_plan_bindings() {
        use asap_types::query_plan::*;
        let entry = |id: u64| QueryPlanEntry {
            language: asap_types::QueryLanguage::PromQl,
            query_id: "sum_over_time(m[1m])".into(),
            canonical_query: "sum_over_time(m[1m])".into(),
            fixed_evaluation: None,
            root: QueryNodeId(1),
            nodes: BTreeMap::from([
                (
                    QueryNodeId(0),
                    QueryPlanNode::ReadMaterialization {
                        binding: MaterializationBinding {
                            materialization: asap_types::PolicyFingerprint(id).into(),
                            stored_output_reference:
                                asap_types::sds::StoredOutputReference::for_definition(
                                    asap_types::PolicyFingerprint(id).into(),
                                ),
                            output_grouping: PhysicalGrouping::PerEntity,
                            item_labels: vec![],
                            window_ms: 5000,
                            pane_origin_ms: Some(0),
                            readout_lookback_ms: Some(60000),
                            full_window_slide_ms: None,
                        },
                    },
                ),
                (
                    QueryNodeId(1),
                    QueryPlanNode::ExactReadout {
                        input: QueryNodeId(0),
                        readout: ExactReadout::Sum,
                    },
                ),
            ]),
            instant: InstantExecution {
                lookback_ms: 60000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        let mut original = physical_plan(1, 1, 0, None);
        let first_entry = entry(1);
        Arc::make_mut(&mut original.query_plan)
            .entries
            .insert(first_entry.canonical_query.clone(), first_entry.clone());
        let first = original
            .readout_program(&first_entry, first_entry.root)
            .unwrap();
        let mut successor = original.clone();
        let second_entry = entry(2);
        Arc::make_mut(&mut successor.query_plan)
            .entries
            .insert(second_entry.canonical_query.clone(), second_entry.clone());
        let second = successor
            .readout_program(&second_entry, second_entry.root)
            .unwrap();
        assert_eq!(
            first.materialization_bindings()[0].materialization,
            asap_types::PolicyFingerprint(1).into()
        );
        assert_eq!(
            second.materialization_bindings()[0].materialization,
            asap_types::PolicyFingerprint(2).into()
        );
    }

    fn dummy_agg(id: u64) -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
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

    /// Build a InstalledPrecomputePlan from a list of marker `id`s. After PR 5
    /// the map key IS the policy fingerprint, derived from
    /// `metric_{id}`. We return both the config and the
    /// dummy-id→fingerprint mapping so the assertions below can look
    /// up entries.
    fn cfg_with_ids(ids: &[u64]) -> (InstalledPrecomputePlan, std::collections::HashMap<u64, u64>) {
        let mut map = HashMap::new();
        let mut id_to_fp = std::collections::HashMap::new();
        for &id in ids {
            let cfg = dummy_agg(id);
            let fp = cfg.policy_fp_u64();
            id_to_fp.insert(id, fp);
            map.insert(fp, cfg);
        }
        (InstalledPrecomputePlan::new(map), id_to_fp)
    }

    #[test]
    fn snapshot_reflects_initial_config() {
        let (cfg, id_to_fp) = cfg_with_ids(&[1, 2, 3]);
        let hr = InstalledPrecomputePlanHandle::new(cfg);
        let snap = hr.snapshot();
        assert_eq!(snap.materializations_by_policy_fingerprint.len(), 3);
        assert!(snap
            .materializations_by_policy_fingerprint
            .contains_key(&id_to_fp[&2]));
    }

    #[test]
    fn swap_replaces_config_atomically() {
        let (cfg1, id_to_fp1) = cfg_with_ids(&[1, 2]);
        let (cfg2, id_to_fp2) = cfg_with_ids(&[3, 4, 5]);
        let hr = InstalledPrecomputePlanHandle::new(cfg1);
        let old = hr.swap(cfg2);
        // Old snapshot still reflects pre-swap contents.
        assert_eq!(old.materializations_by_policy_fingerprint.len(), 2);
        assert!(old
            .materializations_by_policy_fingerprint
            .contains_key(&id_to_fp1[&1]));
        // New snapshot reflects post-swap contents.
        let new_snap = hr.snapshot();
        assert_eq!(new_snap.materializations_by_policy_fingerprint.len(), 3);
        assert!(new_snap
            .materializations_by_policy_fingerprint
            .contains_key(&id_to_fp2[&5]));
        assert!(!new_snap
            .materializations_by_policy_fingerprint
            .contains_key(&id_to_fp1[&1]));
    }

    #[test]
    fn clones_share_underlying_swap() {
        let (cfg1, _) = cfg_with_ids(&[1]);
        let (cfg2, id_to_fp2) = cfg_with_ids(&[2, 3]);
        let hr = InstalledPrecomputePlanHandle::new(cfg1);
        let hr_clone = hr.clone();
        hr.swap(cfg2);
        // The clone sees the swap because both handles share the
        // same ArcSwap inside.
        let snap = hr_clone.snapshot();
        assert_eq!(snap.materializations_by_policy_fingerprint.len(), 2);
        assert!(snap
            .materializations_by_policy_fingerprint
            .contains_key(&id_to_fp2[&3]));
    }

    #[test]
    fn concurrent_readers_see_consistent_snapshot() {
        let (cfg1, _) = cfg_with_ids(&[1, 2]);
        let hr = InstalledPrecomputePlanHandle::new(cfg1);
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
                let n = snap.materializations_by_policy_fingerprint.len();
                assert!(n == 2 || n == 3, "torn snapshot: {n} entries");
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn physical_plan_stages_activates_drains_and_retires() {
        let active = ActivePhysicalPlanHandle::new(physical_plan(7, 1, 100, None));
        let lifecycle = PhysicalPlanLifecycle::new(active.clone());
        lifecycle
            .stage(physical_plan(7, 2, 200, Some(500)), 150)
            .unwrap();

        assert_eq!(active.active_snapshot().plan_version(), 1);
        assert!(matches!(
            lifecycle.activate(7, 2, 199),
            Err(PhysicalPlanLifecycleError::ActivationNotReached { .. })
        ));
        let old = lifecycle.activate(7, 2, 200).unwrap();
        assert_eq!(old.plan_version(), 1);
        assert_eq!(active.active_snapshot().plan_version(), 2);

        let statuses = lifecycle.statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].phase, PhysicalPlanPhase::Draining);
        assert_eq!(statuses[1].phase, PhysicalPlanPhase::Active);
        lifecycle.mark_drained_plan_retired(7, 1);
        assert_eq!(lifecycle.statuses()[0].phase, PhysicalPlanPhase::Retired);
    }

    #[test]
    fn failed_activation_preparation_keeps_active_and_staged_generations() {
        let active = ActivePhysicalPlanHandle::new(physical_plan(7, 1, 100, None));
        let held_reader = active.active_snapshot();
        let lifecycle = PhysicalPlanLifecycle::new(active.clone());
        lifecycle
            .stage(physical_plan(7, 2, 200, None), 150)
            .unwrap();

        let error = lifecycle
            .activate_with_prepare(7, 2, 200, |_| Err("catalog rejected"))
            .unwrap_err();
        assert_eq!(
            error,
            PhysicalPlanLifecycleError::Prepare("catalog rejected".into())
        );
        assert_eq!(active.active_snapshot().plan_version(), 1);
        assert_eq!(held_reader.plan_version(), 1);
        assert!(lifecycle.statuses().iter().any(|status| {
            status.plan_version == 2 && status.phase == PhysicalPlanPhase::Staged
        }));

        lifecycle
            .activate_with_prepare(7, 2, 200, |_| Ok::<(), String>(()))
            .unwrap();
        assert_eq!(active.active_snapshot().plan_version(), 2);
    }

    // Failed publication releases only its staging slot; active readers remain valid.
    #[test]
    fn discard_staged_allows_retry_and_never_discards_active() {
        let active = ActivePhysicalPlanHandle::new(physical_plan(7, 1, 100, None));
        let held_reader = active.active_snapshot();
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
        assert_eq!(active.active_snapshot().plan_version(), 2);
        assert_eq!(held_reader.plan_version(), 1);
    }

    #[test]
    fn materialization_readiness_is_generation_scoped_and_monotonic() {
        let active = ActivePhysicalPlanHandle::new(physical_plan(7, 1, 100, None));
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
        let active = ActivePhysicalPlanHandle::new(physical_plan(7, 2, 100, None));
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
        let active = ActivePhysicalPlanHandle::new(physical_plan(7, 1, 100, None));
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
        assert_eq!(active.active_snapshot().plan_version(), 3);
    }
}
