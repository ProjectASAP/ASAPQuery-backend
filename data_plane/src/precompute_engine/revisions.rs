//! Bounded local-input snapshots and independently published stored results.
//! The checkpoint is runtime state for one installed generation, not a new plan.
use asap_types::sds::{CatalogGeneration, StoredOutputId, StoredOutputReference};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};

pub type Publisher<'a> =
    dyn FnMut(StoredOutputId, Vec<RevisionRecord>) -> Result<(), RevisionError> + 'a;

#[derive(Debug, thiserror::Error)]
#[error("no compatible input snapshot satisfies query freshness")]
pub struct SnapshotUnavailable;

#[derive(Debug, thiserror::Error)]
#[error("revision admission rejected: {0}")]
pub struct AdmissionRejected(pub &'static str);

pub type RevisionError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionPolicy {
    pub correction_horizon_ms: u64,
    pub max_query_staleness_ms: u64,
    pub max_checkpoint_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSample {
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    pub series: String,
    pub timestamp_ms: i64,
    pub first_revision: u64,
    pub value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionRecord {
    pub reference: StoredOutputReference,
    pub group: BTreeMap<String, String>,
    pub start_ms: u64,
    pub end_ms: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    revision: u64,
    captured_at_ms: u64,
    outputs: BTreeMap<u64, Vec<RevisionRecord>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    schema_version: u32,
    generation: CatalogGeneration,
    policy: RevisionPolicy,
    inputs: Vec<InputSample>,
    snapshots: Vec<Snapshot>,
}

/// Owned records pin the selected snapshot independently of later commits/GC.
#[derive(Debug, Clone)]
pub struct PinnedRevision {
    pub revision: u64,
    pub captured_at_ms: u64,
    pub records: Vec<RevisionRecord>,
    pub inputs: Vec<InputSample>,
}

pub struct RevisionStore {
    path: PathBuf,
    _writer: File,
    execution: Mutex<()>,
    state: RwLock<Arc<Checkpoint>>,
    poisoned: std::sync::atomic::AtomicBool,
}

impl RevisionStore {
    pub fn open(
        path: PathBuf,
        generation: CatalogGeneration,
        policy: RevisionPolicy,
    ) -> Result<Self, RevisionError> {
        if policy.correction_horizon_ms == 0
            || policy.max_query_staleness_ms == 0
            || policy.max_checkpoint_bytes == 0
        {
            return Err(
                "revision policy requires positive bounded horizons and a byte budget".into(),
            );
        }
        fs::create_dir_all(
            path.parent()
                .ok_or("revision checkpoint needs a parent directory")?,
        )?;
        let writer = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))?;
        writer.try_lock_exclusive()?;
        if fs::metadata(&path)
            .is_ok_and(|metadata| metadata.len() > policy.max_checkpoint_bytes as u64)
        {
            return Err("revision checkpoint exceeds configured byte budget".into());
        }
        let document = match fs::read(&path) {
            Ok(bytes) => {
                if bytes.len() > policy.max_checkpoint_bytes {
                    return Err("revision checkpoint exceeds configured byte budget".into());
                }
                let document: Checkpoint = serde_json::from_slice(&bytes)?;
                if document.schema_version != 1
                    || document.generation != generation
                    || document.policy != policy
                {
                    return Err(
                        "revision checkpoint differs from installed generation or policy".into(),
                    );
                }
                let mut input_keys = BTreeSet::new();
                for sample in &document.inputs {
                    if sample.timestamp_ms < 0
                        || sample.first_revision == 0
                        || document
                            .snapshots
                            .last()
                            .is_none_or(|s| sample.first_revision > s.revision)
                        || sample.value.is_some_and(|v| !v.is_finite())
                        || !input_keys.insert((&sample.series, sample.timestamp_ms))
                    {
                        return Err("revision checkpoint contains invalid or repeated input".into());
                    }
                }
                let mut previous = (0, 0);
                for snapshot in &document.snapshots {
                    if snapshot.revision <= previous.0 || snapshot.captured_at_ms < previous.1 {
                        return Err("revision checkpoint has non-monotonic snapshots".into());
                    }
                    for (output, records) in &snapshot.outputs {
                        validate_records(*output, records)?;
                    }
                    previous = (snapshot.revision, snapshot.captured_at_ms);
                }
                document
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Checkpoint {
                schema_version: 1,
                generation,
                policy,
                inputs: Vec::new(),
                snapshots: Vec::new(),
            },
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            _writer: writer,
            execution: Mutex::new(()),
            state: RwLock::new(Arc::new(document)),
            poisoned: false.into(),
        })
    }

    fn current(&self) -> Result<Arc<Checkpoint>, RevisionError> {
        if self.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("revision persistence is uncertain; reopen before continuing".into());
        }
        Ok(self
            .state
            .read()
            .map_err(|_| "revision state poisoned")?
            .clone())
    }

    fn persist(&self, next: Checkpoint) -> Result<(), RevisionError> {
        let bytes = serde_json::to_vec(&next)?;
        if bytes.len() > next.policy.max_checkpoint_bytes {
            return Err(Box::new(asap_physical_operators::dag::Error::MemoryLimit));
        }
        let result = (|| -> std::io::Result<()> {
            let temporary = self.path.with_extension("tmp");
            let mut file = File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(temporary, &self.path)?;
            File::open(self.path.parent().unwrap())?.sync_all()
        })();
        if let Err(error) = result {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(error.into());
        }
        *self.state.write().map_err(|_| "revision state poisoned")? = Arc::new(next);
        Ok(())
    }

    /// Admission and execution are serialized; readers only hold the short state
    /// lock. Each output commit is durable independently, allowing old snapshots
    /// to remain readable if a later output fails.
    #[allow(clippy::too_many_arguments)]
    pub fn apply(
        &self,
        generation: &CatalogGeneration,
        samples: Vec<InputSample>,
        now_ms: u64,
        retain_input_ms: u64,
        outputs: &BTreeSet<StoredOutputId>,
        validate: impl FnOnce(&[InputSample], u64) -> Result<(), RevisionError>,
        execute: impl FnOnce(
            &[InputSample],
            u64,
            u64,
            bool,
            &mut Publisher<'_>,
        ) -> Result<(), RevisionError>,
    ) -> Result<u64, RevisionError> {
        let _execution = self
            .execution
            .lock()
            .map_err(|_| "revision execution poisoned")?;
        let current = self.current()?;
        if &current.generation != generation {
            return Err("revision input belongs to a different installed generation".into());
        }
        if current
            .snapshots
            .last()
            .is_some_and(|s| now_ms < s.captured_at_ms)
        {
            return Err("revision clock regressed".into());
        }
        validate(&samples, current.policy.correction_horizon_ms)?;
        if retain_input_ms < current.policy.correction_horizon_ms {
            return Err("input retention is shorter than correction horizon".into());
        }
        let mut inputs: BTreeMap<_, _> = current
            .inputs
            .iter()
            .cloned()
            .map(|s| ((s.series.clone(), s.timestamp_ms), s))
            .collect();
        let mut changed = false;
        let next_revision = current.snapshots.last().map_or(Ok(1), |s| {
            s.revision.checked_add(1).ok_or("input revision exhausted")
        })?;
        for mut sample in samples {
            if sample.value.is_some_and(|v| !v.is_finite()) {
                return Err("revision input must be finite or an explicit stale marker".into());
            }
            let key = (sample.series.clone(), sample.timestamp_ms);
            sample.first_revision = inputs
                .get(&key)
                .map_or(next_revision, |old| old.first_revision);
            match inputs.get(&key) {
                Some(old) if old != &sample => {
                    return Err(AdmissionRejected("conflicting sample in revision input").into())
                }
                Some(_) => {}
                None => {
                    inputs.insert(key, sample);
                    changed = true;
                }
            }
        }
        let pending = current
            .snapshots
            .last()
            .is_some_and(|s| outputs.iter().any(|o| !s.outputs.contains_key(&o.0)));
        if changed && pending {
            return Err(
                "prior input revision is pending; retry its execution before new admission".into(),
            );
        }
        let revision = if changed
            || current.snapshots.is_empty()
            || (!pending
                && current
                    .snapshots
                    .last()
                    .is_some_and(|s| now_ms > s.captured_at_ms))
        {
            let revision = current.snapshots.last().map_or(Ok(1), |s| {
                s.revision.checked_add(1).ok_or("input revision exhausted")
            })?;
            let floor = now_ms.saturating_sub(retain_input_ms);
            let mut next = (*current).clone();
            next.inputs = inputs
                .into_values()
                .filter(|s| s.timestamp_ms >= 0 && s.timestamp_ms as u64 >= floor)
                .collect();
            let cutoff = now_ms.saturating_sub(current.policy.max_query_staleness_ms);
            next.snapshots.retain(|s| s.captured_at_ms >= cutoff);
            next.snapshots.push(Snapshot {
                revision,
                captured_at_ms: now_ms,
                outputs: BTreeMap::new(),
            });
            self.persist(next)?;
            revision
        } else if !pending {
            return Ok(current.snapshots.last().unwrap().revision);
        } else {
            current.snapshots.last().unwrap().revision
        };
        let snapshot = self.current()?;
        let mut publish = |output: StoredOutputId, records: Vec<RevisionRecord>| {
            if !outputs.contains(&output) {
                return Err("revision publication references an uninstalled output".into());
            }
            validate_records(output.0, &records)?;
            let mut next = (*self.current()?).clone();
            let frame = next
                .snapshots
                .last_mut()
                .ok_or("input revision disappeared")?;
            if frame.revision != revision {
                return Err("input revision changed during execution".into());
            }
            // Reuse committed randomized sketches on recovery; do not recompute
            // their identity from another random realization of the same input.
            if frame.outputs.contains_key(&output.0) {
                return Ok(());
            }
            frame.outputs.insert(output.0, records);
            self.persist(next)
        };
        execute(
            &snapshot.inputs,
            revision,
            snapshot.snapshots.last().unwrap().captured_at_ms,
            changed,
            &mut publish,
        )?;
        if outputs.iter().any(|o| {
            !self
                .current()
                .map(|s| s.snapshots.last().unwrap().outputs.contains_key(&o.0))
                .unwrap_or(false)
        }) {
            return Err("revision execution did not publish every selected output".into());
        }
        Ok(revision)
    }

    pub(crate) fn committed_outputs(
        &self,
        revision: u64,
    ) -> Result<BTreeMap<u64, Vec<RevisionRecord>>, RevisionError> {
        self.current()?
            .snapshots
            .iter()
            .find(|s| s.revision == revision)
            .map(|s| s.outputs.clone())
            .ok_or_else(|| "captured revision disappeared".into())
    }

    #[cfg(test)]
    pub fn pin(
        &self,
        required: &BTreeSet<StoredOutputId>,
        now_ms: u64,
    ) -> Result<PinnedRevision, RevisionError> {
        self.pin_matching(required, now_ms, |_| true)
    }

    fn pin_matching(
        &self,
        required: &BTreeSet<StoredOutputId>,
        now_ms: u64,
        eligible: impl Fn(&BTreeMap<u64, Vec<RevisionRecord>>) -> bool,
    ) -> Result<PinnedRevision, RevisionError> {
        let state = self.current()?;
        let snapshot = state
            .snapshots
            .iter()
            .rev()
            .find(|s| {
                s.captured_at_ms <= now_ms
                    && now_ms - s.captured_at_ms <= state.policy.max_query_staleness_ms
                    && required.iter().all(|o| s.outputs.contains_key(&o.0))
                    && eligible(&s.outputs)
            })
            .ok_or(SnapshotUnavailable)?;
        Ok(PinnedRevision {
            revision: snapshot.revision,
            captured_at_ms: snapshot.captured_at_ms,
            inputs: state
                .inputs
                .iter()
                .filter(|s| s.first_revision <= snapshot.revision)
                .cloned()
                .collect(),
            records: required
                .iter()
                .flat_map(|o| snapshot.outputs[&o.0].clone())
                .collect(),
        })
    }
}

fn validate_records(output: u64, records: &[RevisionRecord]) -> Result<(), RevisionError> {
    let mut keys = BTreeSet::new();
    for record in records {
        record.reference.validate()?;
        if record.reference.stored_output_id.0 != output
            || record.start_ms >= record.end_ms
            || record.payload.is_empty()
            || !keys.insert((&record.group, record.start_ms, record.end_ms))
        {
            return Err("invalid or duplicate revision output record".into());
        }
    }
    Ok(())
}

use crate::storage_engines::types::{
    AggregateCore, InstalledPrecomputePlanHandle, RuntimePhysicalPlan,
};
use asap_physical_operators::values::{Batch, Schema, Value};
use asap_summary_state::stored_state::native;
use planner_types::post_asap::{SummaryFamilyType, SummaryField, SummarySchema};

fn state_schema(family: SummaryFamilyType) -> Schema {
    Arc::new(SummarySchema {
        fields: vec![SummaryField {
            name: "state".into(),
            dtype: family,
            nullable: false,
        }],
        time_index: None,
    })
}

pub(crate) fn encode_state(
    state: Arc<dyn AggregateCore>,
    family: SummaryFamilyType,
) -> Result<Vec<u8>, RevisionError> {
    Ok(native::encode_batch(&Batch::try_new(
        state_schema(family.clone()),
        vec![vec![Value::Summary {
            family,
            state: asap_summary_state::physical::to_physical(state.as_ref())?,
        }]],
    )?)?)
}

pub(crate) fn decode_state(
    record: &RevisionRecord,
    config: &asap_types::PrecomputeMaterialization,
) -> Result<Arc<dyn AggregateCore>, RevisionError> {
    let family = config.accumulator_spec()?.family;
    let batch = native::decode_batch(&record.payload, state_schema(family), record.payload.len())?;
    match batch.rows() {
        [row] => match row.as_slice() {
            [Value::Summary { state, .. }] => Ok(Arc::from(
                asap_summary_state::physical::from_physical(state.as_ref())?,
            )),
            _ => Err("revision record must contain exactly one typed summary".into()),
        },
        _ => Err("revision record must contain exactly one row".into()),
    }
}

/// Decode the complete persisted output schema selected by Planner. A native
/// batch may carry many groups; it is not a scalar accumulator record.
pub(crate) fn decode_native_record(
    plan: &RuntimePhysicalPlan,
    record: &RevisionRecord,
    max_bytes: usize,
) -> Result<Option<Batch>, RevisionError> {
    use asap_types::executable_plan::BackendNodeBinding;
    let mut expected = None;
    for installed in plan.precompute_plan.executable_dags.values() {
        for sink in installed.native_programs.keys() {
            if !matches!(installed.binding.node(*sink), Some(BackendNodeBinding::Materialization { stored_output }) if *stored_output == record.reference.stored_output_id)
            {
                continue;
            }
            let program = installed
                .native_program(*sink)?
                .ok_or("native output program missing")?;
            let schema = program.output_contract(u64::from(sink.0))?.schema;
            if asap_physical_operators::physical_planner::precompute::is_population_schema(&schema)
            {
                continue;
            }
            if expected
                .as_ref()
                .is_some_and(|previous| previous != &schema)
            {
                return Err("shared native output has conflicting schemas".into());
            }
            expected = Some(schema);
        }
    }
    let Some(schema) = expected else {
        return Ok(None);
    };
    if !record.group.is_empty() {
        return Err("native revision must contain the whole grouped output".into());
    }
    if record.payload.len() > max_bytes {
        return Err(asap_physical_operators::Error::MemoryLimit.into());
    }
    let batch = native::decode_batch(&record.payload, schema, max_bytes)?;
    if batch.bytes() > max_bytes {
        return Err(asap_physical_operators::Error::MemoryLimit.into());
    }
    Ok(Some(batch))
}

/// A single local owner serializes input revisions for the installed plan.
/// Opening is lazy so activation/recovery does not depend on receiving new data.
pub struct RevisionRuntime {
    directory: PathBuf,
    pub policy: RevisionPolicy,
    plans: InstalledPrecomputePlanHandle,
    current: Mutex<Option<(CatalogGeneration, Arc<RevisionStore>)>>,
}

impl RevisionRuntime {
    pub fn new(
        directory: PathBuf,
        policy: RevisionPolicy,
        plans: InstalledPrecomputePlanHandle,
    ) -> Self {
        Self {
            directory,
            policy,
            plans,
            current: Mutex::new(None),
        }
    }

    pub(crate) fn installed(
        &self,
    ) -> Result<(Arc<RuntimePhysicalPlan>, Arc<RevisionStore>), RevisionError> {
        let plan = self
            .plans
            .active_physical_plan_snapshot()
            .ok_or("revision execution requires an active physical plan")?;
        if plan.precompute_plan.ingest.protocol
            != asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
        {
            return Err("continuous revisions require the local Remote Write input binding".into());
        }
        if plan
            .query_plan
            .entries
            .values()
            .any(|entry| entry.instant.full_history && !entry.materialization_bindings().is_empty())
        {
            return Err("continuous revisions require a bounded query history".into());
        }
        let generation = plan
            .precompute_plan
            .summary_catalog
            .as_ref()
            .ok_or("revision execution requires a catalog generation")?;
        let mut current = self
            .current
            .lock()
            .map_err(|_| "revision installation poisoned")?;
        if let Some((previous, store)) = current
            .as_ref()
            .filter(|(previous, _)| previous == generation)
        {
            let _ = previous;
            return Ok((Arc::clone(&plan), Arc::clone(store)));
        }
        // A selected producer must have a typed recovery codec before any raw
        // input is durably admitted. Codec failure is a deployment-format error,
        // never a reason to leave an accepted input revision unrecoverable.
        for config in plan
            .precompute_plan
            .materializations
            .iter()
            .filter(|c| c.derived_input.is_none())
        {
            let program = super::raw_dag::RawDagProgram::from_plan(&plan.precompute_plan, config)?;
            let bytes = encode_state(
                Arc::from(program.updater()?.into_accumulator()),
                program.family,
            )?;
            if bytes.len() > self.policy.max_checkpoint_bytes {
                return Err(Box::new(asap_physical_operators::Error::MemoryLimit));
            }
        }
        let path = self.directory.join(format!(
            "{}-{}.json",
            generation.plan_id, generation.plan_version
        ));
        let store = Arc::new(RevisionStore::open(
            path,
            generation.clone(),
            self.policy.clone(),
        )?);
        // Validate recovered bindings and payloads before making any result readable.
        for snapshot in &store.current()?.snapshots {
            for (output, records) in &snapshot.outputs {
                let config = plan
                    .precompute_plan
                    .materializations
                    .iter()
                    .find(|c| c.policy_fp_u64() == *output)
                    .ok_or("recovered revision has an uninstalled output")?;
                let reference = plan
                    .installed_precompute_plan
                    .stored_output_reference(StoredOutputId(*output))
                    .ok_or("recovered revision has no installed binding")?;
                for record in records {
                    if record.reference != reference {
                        return Err(
                            "recovered revision semantic binding differs from installed plan"
                                .into(),
                        );
                    }
                    if decode_native_record(&plan, record, self.policy.max_checkpoint_bytes)?
                        .is_none()
                    {
                        decode_state(record, config)?;
                    }
                }
            }
        }
        *current = Some((generation.clone(), Arc::clone(&store)));
        Ok((plan, store))
    }

    pub(crate) fn query_view(
        &self,
        required: &BTreeSet<StoredOutputId>,
        now_ms: u64,
        ranges: &[(StoredOutputId, u64, u64)],
        expected: &CatalogGeneration,
    ) -> Result<crate::storage_engines::sketch_db::index::SketchStore, RevisionError> {
        let (plan, store) = self.installed()?;
        if plan.precompute_plan.summary_catalog.as_ref() != Some(expected) {
            return Err("query snapshot generation differs from the selected QueryPlan".into());
        }
        let mut pinned = store.pin_matching(required, now_ms, |outputs| {
            ranges
                .iter()
                .all(|(output, start, end)| records_cover(&outputs[&output.0], *start, *end))
        })?;
        pinned.records.retain(|r| {
            ranges.iter().any(|(output, start, end)| {
                r.reference.stored_output_id == *output && r.start_ms >= *start && r.end_ms <= *end
            })
        });
        crate::storage_engines::sketch_db::index::SketchStore::from_revision(&plan, &pinned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn generation() -> CatalogGeneration {
        CatalogGeneration {
            schema_version: 6,
            plan_id: 1,
            plan_version: 1,
            snapshot_sha256: "a".repeat(64),
        }
    }
    fn policy() -> RevisionPolicy {
        RevisionPolicy {
            correction_horizon_ms: 1000,
            max_query_staleness_ms: 100,
            max_checkpoint_bytes: 100_000,
        }
    }
    fn sample(time: i64, value: f64) -> InputSample {
        InputSample {
            metric: "x".into(),
            labels: BTreeMap::new(),
            series: "x".into(),
            timestamp_ms: time,
            first_revision: 0,
            value: Some(value),
        }
    }
    fn record(output: u64, value: u8) -> RevisionRecord {
        RevisionRecord {
            reference: StoredOutputReference {
                stored_output_id: StoredOutputId(output),
                definition_id: serde_json::from_value(serde_json::json!(format!(
                    "sds-v1:{}",
                    "a".repeat(64)
                )))
                .unwrap(),
            },
            group: BTreeMap::new(),
            start_ms: 0,
            end_ms: 100,
            payload: vec![value],
        }
    }
    fn output_set() -> BTreeSet<StoredOutputId> {
        BTreeSet::from([StoredOutputId(1), StoredOutputId(2)])
    }

    /// A durable partial r2 cannot force a two-branch query to mix r1 and r2.
    #[test]
    fn partial_revision_restart_and_pinned_read_are_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let store = RevisionStore::open(path.clone(), generation(), policy()).unwrap();
        let outputs = output_set();
        store
            .apply(
                &generation(),
                vec![sample(10, 2.)],
                100,
                1000,
                &outputs,
                |_, _| Ok(()),
                |_, _, _, _, publish| {
                    publish(StoredOutputId(1), vec![record(1, 2)])?;
                    publish(StoredOutputId(2), vec![record(2, 2)])
                },
            )
            .unwrap();
        let old = store.pin(&outputs, 100).unwrap();
        let failure = store
            .apply(
                &generation(),
                vec![sample(20, 3.)],
                110,
                1000,
                &outputs,
                |_, _| Ok(()),
                |_, _, _, _, publish| {
                    publish(StoredOutputId(1), vec![record(1, 5)])?;
                    Err(Box::new(asap_physical_operators::Error::Cancelled))
                },
            )
            .unwrap_err();
        assert!(matches!(
            failure.downcast_ref(),
            Some(asap_physical_operators::Error::Cancelled)
        ));
        assert_eq!(store.pin(&outputs, 110).unwrap().revision, 1);
        assert_eq!(
            store
                .pin(&BTreeSet::from([StoredOutputId(1)]), 110)
                .unwrap()
                .revision,
            2
        );
        drop(store);
        let store = RevisionStore::open(path.clone(), generation(), policy()).unwrap();
        assert_eq!(store.pin(&outputs, 110).unwrap().revision, 1);
        store
            .apply(
                &generation(),
                vec![],
                120,
                1000,
                &outputs,
                |_, _| Ok(()),
                |inputs, revision, captured, _, publish| {
                    assert_eq!((inputs.len(), revision, captured), (2, 2, 110));
                    // Already committed bytes survive retry, including randomized sketches.
                    publish(StoredOutputId(1), vec![record(1, 99)])?;
                    publish(StoredOutputId(2), vec![record(2, 5)])
                },
            )
            .unwrap();
        let new = store.pin(&outputs, 120).unwrap();
        assert_eq!(new.revision, 2);
        assert_eq!(old.inputs.len(), 1);
        assert_eq!(new.inputs.len(), 2);
        assert_eq!(
            new.records.iter().map(|r| r.payload[0]).collect::<Vec<_>>(),
            vec![5, 5]
        );
        assert_eq!(
            old.records.iter().map(|r| r.payload[0]).collect::<Vec<_>>(),
            vec![2, 2]
        );
        assert!(store.pin(&outputs, 211).is_err());
        drop(store);
        let mut next = generation();
        next.plan_version += 1;
        assert!(RevisionStore::open(path, next, policy()).is_err());
    }

    /// Newer published outputs without the requested historical pane must not
    /// hide an older fresh snapshot that still has complete coverage.
    #[test]
    fn coverage_is_part_of_common_snapshot_selection() {
        let dir = tempfile::tempdir().unwrap();
        let store = RevisionStore::open(dir.path().join("checkpoint.json"), generation(), policy())
            .unwrap();
        let outputs = output_set();
        for (time, value) in [(100, 2), (110, 3)] {
            store
                .apply(
                    &generation(),
                    vec![sample(time as i64 - 1, value as f64)],
                    time,
                    1000,
                    &outputs,
                    |_, _| Ok(()),
                    |_, _, _, _, publish| {
                        for output in &outputs {
                            let mut record = record(output.0, value);
                            if time == 110 {
                                record.start_ms = 100;
                                record.end_ms = 110;
                            }
                            publish(*output, vec![record])?;
                        }
                        Ok(())
                    },
                )
                .unwrap();
        }
        let eligible = |records: &BTreeMap<u64, Vec<RevisionRecord>>| {
            outputs
                .iter()
                .all(|o| records_cover(&records[&o.0], 0, 100))
        };
        assert_eq!(
            store
                .pin_matching(&outputs, 110, eligible)
                .unwrap()
                .revision,
            1
        );
        assert!(store.pin_matching(&outputs, 201, eligible).is_err());
    }

    /// Local capture must evaluate newly elapsed windows without inventing a
    /// later source sample or replaying an already admitted input as new data.
    #[test]
    fn periodic_capture_reuses_input_without_new_admission() {
        let dir = tempfile::tempdir().unwrap();
        let store = RevisionStore::open(dir.path().join("checkpoint.json"), generation(), policy())
            .unwrap();
        let outputs = output_set();
        store
            .apply(
                &generation(),
                vec![sample(5, 2.)],
                10,
                1000,
                &outputs,
                |_, _| Ok(()),
                |_, _, _, admitted, publish| {
                    assert!(admitted);
                    for output in &outputs {
                        publish(*output, vec![])?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        store
            .apply(
                &generation(),
                vec![],
                100,
                1000,
                &outputs,
                |_, _| Ok(()),
                |inputs, revision, captured, admitted, publish| {
                    assert_eq!((inputs.len(), revision, captured), (1, 2, 100));
                    assert!(!admitted);
                    for output in &outputs {
                        publish(*output, vec![record(output.0, 2)])?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(store.pin(&outputs, 100).unwrap().revision, 2);
    }

    /// Rejection must not persist the valid prefix, a dedup key, or a revision.
    #[test]
    fn rejected_whole_batch_and_budget_leave_checkpoint_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let store = RevisionStore::open(path.clone(), generation(), policy()).unwrap();
        let outputs = output_set();
        store
            .apply(
                &generation(),
                vec![sample(10, 2.)],
                100,
                1000,
                &outputs,
                |_, _| Ok(()),
                |_, _, _, _, publish| {
                    for o in &outputs {
                        publish(*o, vec![record(o.0, 2)])?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(store
            .apply(
                &generation(),
                vec![sample(20, 3.), sample(10, 99.)],
                110,
                1000,
                &outputs,
                |_, _| Ok(()),
                |_, _, _, _, _| panic!("rejected batch executed")
            )
            .is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(store
            .apply(
                &generation(),
                vec![sample(20, 3.)],
                110,
                1000,
                &outputs,
                |_, _| Err("outside correction horizon".into()),
                |_, _, _, _, _| panic!("rejected batch executed")
            )
            .is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        let mut oversized = sample(20, 3.);
        oversized.series = "x".repeat(100_000);
        let error = store
            .apply(
                &generation(),
                vec![oversized],
                110,
                1000,
                &outputs,
                |_, _| Ok(()),
                |_, _, _, _, _| panic!("over-budget batch executed"),
            )
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref(),
            Some(asap_physical_operators::Error::MemoryLimit)
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}

pub(crate) fn pin_query(
    index: &crate::storage_engines::sketch_db::index::SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    times: &[u64],
    generation: Option<&CatalogGeneration>,
) -> Result<
    Option<crate::storage_engines::sketch_db::index::SketchStore>,
    crate::query_engines::EngineError,
> {
    use crate::query_engines::EngineError;
    let required: BTreeSet<_> = entry
        .materialization_bindings()
        .into_iter()
        .map(|b| b.stored_output_reference.stored_output_id)
        .collect();
    let current_series =
        entry.nodes.values().any(|node| {
            matches!(
                node,
                asap_types::query_plan::QueryPlanNode::Logical {
                    operator:
                        asap_types::query_plan::query_time::QueryTimeOperator::CurrentSeries { .. },
                    ..
                }
            )
        });
    if required.is_empty() && !current_series {
        return Ok(None);
    }
    let runtime = index
        .revisions
        .read()
        .map_err(|_| {
            EngineError::Physical(asap_physical_operators::Error::Invalid(
                "revision installation poisoned".into(),
            ))
        })?
        .clone();
    let Some(runtime) = runtime else {
        return Ok(None);
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| EngineError::Physical(asap_physical_operators::Error::Invalid(e.to_string())))?
        .as_millis() as u64;
    let ranges: Vec<_> = entry
        .materialization_bindings()
        .into_iter()
        .flat_map(|binding| {
            times.iter().map(move |time| {
                (
                    binding.materialization,
                    time.saturating_sub(
                        binding
                            .readout_lookback_ms
                            .unwrap_or(entry.instant.lookback_ms),
                    ),
                    *time,
                )
            })
        })
        .collect();
    runtime
        .query_view(
            &required,
            now,
            &ranges,
            generation.ok_or_else(|| {
                EngineError::Physical(asap_physical_operators::Error::Invalid(
                    "query revision requires a catalog generation".into(),
                ))
            })?,
        )
        .map(Some)
        .map_err(|error| {
            if error.is::<SnapshotUnavailable>() {
                return EngineError::capability_miss("summary_snapshot", error.to_string());
            }
            match error.downcast::<asap_physical_operators::Error>() {
                Ok(native) => EngineError::Physical(*native),
                Err(error) => EngineError::Physical(asap_physical_operators::Error::Invalid(
                    error.to_string(),
                )),
            }
        })
}

fn records_cover(records: &[RevisionRecord], start: u64, end: u64) -> bool {
    let mut groups = BTreeMap::<&BTreeMap<String, String>, Vec<(u64, u64)>>::new();
    for record in records {
        let windows = groups.entry(&record.group).or_default();
        if record.start_ms >= start && record.end_ms <= end {
            windows.push((record.start_ms, record.end_ms));
        }
    }
    !groups.is_empty()
        && groups.values_mut().all(|windows| {
            windows.sort_unstable();
            let mut cursor = start;
            for (lo, hi) in windows {
                if *hi <= cursor {
                    continue;
                }
                if *lo != cursor {
                    return false;
                }
                cursor = *hi;
            }
            cursor == end
        })
}
