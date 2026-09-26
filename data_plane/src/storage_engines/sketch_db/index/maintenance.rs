//! Immutable maintenance inputs read from the existing durable summary tier.
//!
//! Unlike query fallback helpers, this path must propagate corrupt/missing part
//! errors: silently omitting a source window would permanently corrupt an outer
//! summary. Completion is an admission barrier, not merely an emitted flag.
use super::*;
use crate::storage_engines::types::AggregateCore;

pub(crate) struct FrozenExactWindows {
    pub(crate) stored_output_reference: asap_types::sds::StoredOutputReference,
    pub(crate) storage_handle: u64,
    pub(crate) definition: StoredOutputId,
    pub(crate) generation: Arc<CatalogGeneration>,
    pub(crate) group: BTreeMap<String, String>,
    pub(crate) windows: BTreeMap<(u64, u64), Arc<dyn AggregateCore>>,
    pub(crate) singleton_population_complete: bool,
}

/// Store-issued evidence that every durable raw population contributes the
/// same window. This is deliberately not serializable or caller-constructible.
pub(crate) struct CompleteRawMaintenanceCohort {
    inputs: Vec<FrozenExactWindows>,
    window: (u64, u64),
}

impl CompleteRawMaintenanceCohort {
    pub(crate) fn inputs(&self) -> &[FrozenExactWindows] {
        &self.inputs
    }
}

enum FrozenPublicationInputs<'a> {
    Requested(&'a [FrozenExactWindows]),
    Complete(&'a CompleteRawMaintenanceCohort),
}

impl FrozenPublicationInputs<'_> {
    fn inputs(&self) -> &[FrozenExactWindows] {
        match self {
            Self::Requested(inputs) => inputs,
            Self::Complete(cohort) => cohort.inputs(),
        }
    }
}

impl SketchStore {
    /// Acquire a complete immutable read set before any output is reserved.
    /// Individual payloads are immutable; checking the shared generation again
    /// after all reads prevents a catalog transition from mixing incarnations.
    pub(crate) fn read_frozen_exact_cohort(
        &self,
        generation: &Arc<CatalogGeneration>,
        expected_definitions: &BTreeSet<StoredOutputId>,
        requests: &[(
            u64,
            StoredOutputId,
            BTreeSet<(u64, u64)>,
            BTreeMap<String, String>,
        )],
    ) -> Result<Vec<FrozenExactWindows>, String> {
        if requests.is_empty() || requests.len() > 65_536 {
            return Err("immutable input cohort has invalid population count".into());
        }
        let supplied = requests
            .iter()
            .map(|(_, definition, _, _)| *definition)
            .collect();
        if expected_definitions != &supplied {
            return Err("immutable input cohort differs from installed input definitions".into());
        }
        let mut ordered: Vec<_> = requests.iter().collect();
        ordered.sort_by(|left, right| (left.1, left.0, &left.3).cmp(&(right.1, right.0, &right.3)));
        if ordered
            .windows(2)
            .any(|pair| (pair[0].1, pair[0].0, &pair[0].3) == (pair[1].1, pair[1].0, &pair[1].3))
        {
            return Err("immutable input cohort repeats a physical population".into());
        }
        self.validate_routed_catalog_generation(Some(generation.as_ref()))?;
        let inputs = ordered
            .into_iter()
            .map(|(sid, definition, windows, group)| {
                self.read_frozen_exact_windows(*sid, *definition, generation, windows, group)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.validate_routed_catalog_generation(Some(generation.as_ref()))?;
        Ok(inputs)
    }

    pub(crate) fn read_complete_raw_maintenance_cohort(
        &self,
        generation: &Arc<CatalogGeneration>,
        definitions: &BTreeSet<StoredOutputId>,
        window: (u64, u64),
    ) -> Result<CompleteRawMaintenanceCohort, String> {
        if definitions.is_empty() || window.0 >= window.1 {
            return Err("complete raw cohort has an empty definition set or window".into());
        }
        let mut requests = Vec::new();
        for definition in definitions {
            let populations = self.complete_raw_maintenance_population(*definition, generation)?;
            for (sid, groups) in populations {
                for (group, windows) in groups {
                    if !windows.contains(&window) {
                        return Err("complete raw cohort is missing a population window".into());
                    }
                    requests.push((sid, *definition, BTreeSet::from([window]), group));
                }
            }
        }
        let inputs = self.read_frozen_exact_cohort(generation, definitions, &requests)?;
        Ok(CompleteRawMaintenanceCohort { inputs, window })
    }

    fn durable_maintenance_population_ids(
        &self,
        definition: StoredOutputId,
    ) -> Result<BTreeSet<u64>, String> {
        let metadata = self
            .persistence_metadata
            .read()
            .map_err(|_| "persistence metadata registry poisoned")?;
        let writer = metadata
            .as_ref()
            .ok_or("immutable population proof requires durable metadata")?;
        // Retained populations remain a completeness fence even when their
        // generation is no longer readable. Losing their binding cannot certify
        // that a newly observed subset is the complete source population.
        Ok(writer
            .load_strict()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|record| !record.removed && record.stored_output_id == Some(definition))
            .map(|record| record.storage_handle)
            .collect())
    }

    /// Enumerate durable source coordinates; the consumer revalidates exact
    /// coverage and incarnation before reading or publishing any state.
    pub(crate) fn completed_maintenance_coordinates(
        &self,
        definition: StoredOutputId,
        generation: &CatalogGeneration,
    ) -> Result<BTreeMap<u64, BTreeMap<BTreeMap<String, String>, BTreeSet<(u64, u64)>>>, String>
    {
        self.maintenance_coordinates(definition, generation, false)
    }

    /// Prove the complete raw population before a group-aware consumer joins it.
    /// Historical durable SIDs cannot disappear merely because restart did not
    /// bind them to the current catalog.
    pub(crate) fn complete_raw_maintenance_population(
        &self,
        definition: StoredOutputId,
        generation: &CatalogGeneration,
    ) -> Result<BTreeMap<u64, BTreeMap<BTreeMap<String, String>, BTreeSet<(u64, u64)>>>, String>
    {
        self.maintenance_coordinates(definition, generation, true)
    }

    fn maintenance_coordinates(
        &self,
        definition: StoredOutputId,
        generation: &CatalogGeneration,
        require_complete_population: bool,
    ) -> Result<BTreeMap<u64, BTreeMap<BTreeMap<String, String>, BTreeSet<(u64, u64)>>>, String>
    {
        let admission = self
            .admission
            .read()
            .map_err(|_| "admission registry poisoned")?;
        self.validate_routed_catalog_generation(Some(generation))?;
        if require_complete_population && !admission.is_finite_complete() {
            return Err("complete raw population requires a finite source closure".into());
        }
        let handle = self
            .persistence_read
            .read()
            .map_err(|_| "persistence registry poisoned")?
            .clone()
            .ok_or("immutable maintenance requires durable input state")?;
        let instances = self
            .instances
            .read()
            .map_err(|_| "instance registry poisoned")?;
        let mut population_ids = self.durable_maintenance_population_ids(definition)?;
        population_ids.extend(
            instances
                .iter()
                .filter(|(_, binding)| {
                    binding.metadata.policy_fp == definition.fingerprint()
                        && binding.stored_output_reference
                            == self.descriptors.stored_output_reference(definition)
                        && binding.catalog_generation.as_deref() == Some(generation)
                })
                .map(|(sid, _)| *sid),
        );
        if !require_complete_population && population_ids.len() > 1 {
            return Err("immutable maintenance requires one durable physical population".into());
        }
        let completed = self
            .completed_windows
            .read()
            .map_err(|_| "completion registry poisoned")?;
        if require_complete_population {
            if population_ids.is_empty() {
                return Err("raw population has no durable physical instances".into());
            }
            for sid in &population_ids {
                let binding = instances
                    .get(sid)
                    .ok_or("durable population SID is not bound in this catalog")?;
                if binding.catalog_generation.as_deref() != Some(generation)
                    || !binding.metadata.is_writable()
                    || matches!(
                        binding.data_descriptor.source,
                        asap_types::sds::DataSourceIdentity::Derived { .. }
                    )
                    || !completed.contains_key(sid)
                {
                    return Err("raw population contains an incomplete or foreign lifetime".into());
                }
            }
        }
        let mut coordinates = BTreeMap::new();
        for (sid, binding) in instances.iter() {
            if binding.metadata.policy_fp != definition.fingerprint()
                || binding.stored_output_reference
                    != self.descriptors.stored_output_reference(definition)
                || binding.catalog_generation.as_deref() != Some(generation)
                || !binding.metadata.is_writable()
            {
                continue;
            }
            let Some(end) = completed.get(sid).copied() else {
                continue;
            };
            let keys: Vec<_> = binding.metadata.group_by_keys.iter().cloned().collect();
            for part in handle.manifest.live_parts_overlapping(0, end) {
                let reader = handle
                    .part_cache
                    .get_or_load(part.part_id)
                    .map_err(|e| e.to_string())?;
                for record in reader.index_records() {
                    if record.agg_id != *sid || record.end_ts > end {
                        continue;
                    }
                    let entry = reader.load_entry(&record).map_err(|e| e.to_string())?;
                    if entry.label.as_ref().map_or(0, |label| label.labels.len()) != keys.len() {
                        return Err(
                            "immutable input label arity differs from its descriptor".into()
                        );
                    }
                    coordinates
                        .entry(*sid)
                        .or_insert_with(BTreeMap::new)
                        .entry(Self::rebuild_label_map(&keys, &entry.label))
                        .or_insert_with(BTreeSet::new)
                        .insert((record.start_ts, record.end_ts));
                }
            }
        }
        if require_complete_population
            && coordinates.keys().copied().collect::<BTreeSet<_>>() != population_ids
        {
            return Err("completed raw population is missing durable payload coordinates".into());
        }
        Ok(coordinates)
    }

    pub(crate) fn read_frozen_exact_windows(
        &self,
        sid: u64,
        definition: StoredOutputId,
        generation: &Arc<CatalogGeneration>,
        expected_windows: &BTreeSet<(u64, u64)>,
        group: &BTreeMap<String, String>,
    ) -> Result<FrozenExactWindows, String> {
        let start_ms = expected_windows
            .iter()
            .map(|window| window.0)
            .min()
            .ok_or("immutable input has no requested windows")?;
        let end_ms = expected_windows
            .iter()
            .map(|window| window.1)
            .max()
            .unwrap();
        if expected_windows.iter().any(|(start, end)| start >= end) || end_ms > i64::MAX as u64 {
            return Err("invalid immutable input window".into());
        }
        self.validate_routed_catalog_generation(Some(generation.as_ref()))?;
        // Do not retain either lock while opening parts. The immutable frontier
        // is monotone, and final publication rechecks the catalog incarnation.
        let admission = self
            .admission
            .read()
            .map_err(|_| "admission registry poisoned")?;
        let (keys, singleton_population_complete, stored_output_reference) = {
            let bindings = self
                .instances
                .read()
                .map_err(|_| "instance registry poisoned")?;
            let binding = bindings.get(&sid).ok_or("immutable input SID is absent")?;
            if binding.metadata.policy_fp != definition.fingerprint()
                || binding.stored_output_reference
                    != self.descriptors.stored_output_reference(definition)
                || binding.catalog_generation.as_deref() != Some(generation.as_ref())
                || binding.metadata.status() == AggStatus::Expired
            {
                return Err("immutable input identity or lifetime differs".into());
            }
            let mut population_ids = self.durable_maintenance_population_ids(definition)?;
            population_ids.extend(
                bindings
                    .iter()
                    .filter(|(_, candidate)| {
                        candidate.metadata.policy_fp == definition.fingerprint()
                            && candidate.stored_output_reference
                                == self.descriptors.stored_output_reference(definition)
                            && candidate.catalog_generation.as_deref() == Some(generation.as_ref())
                    })
                    .map(|(sid, _)| *sid),
            );
            let singleton = admission.is_finite_complete()
                && !matches!(
                    binding.data_descriptor.source,
                    asap_types::sds::DataSourceIdentity::Derived { .. }
                )
                && population_ids == BTreeSet::from([sid]);
            (
                binding.metadata.group_by_keys.clone(),
                singleton,
                binding
                    .stored_output_reference
                    .clone()
                    .ok_or("immutable input has no stored-output binding")?,
            )
        };
        drop(admission);
        if group.keys().cloned().collect::<BTreeSet<_>>() != keys {
            return Err("immutable input population does not match its descriptor".into());
        }
        let keys: Vec<_> = keys.into_iter().collect();
        if self
            .completed_windows
            .read()
            .map_err(|_| "completion registry poisoned")?
            .get(&sid)
            .is_none_or(|frontier| *frontier < end_ms)
        {
            return Err("immutable input window is not complete".into());
        }
        let handle = self
            .persistence_read
            .read()
            .map_err(|_| "persistence registry poisoned")?
            .clone()
            .ok_or("immutable maintenance requires durable input state")?;
        let mut windows = BTreeMap::new();
        let mut observed_populations = BTreeSet::new();
        for part in handle.manifest.live_parts_overlapping(start_ms, end_ms) {
            let reader = handle
                .part_cache
                .get_or_load(part.part_id)
                .map_err(|e| e.to_string())?;
            for record in reader.index_records() {
                if record.agg_id != sid || record.start_ts < start_ms || record.end_ts > end_ms {
                    continue;
                }
                let entry = reader.load_entry(&record).map_err(|e| e.to_string())?;
                if entry.label.as_ref().map_or(0, |label| label.labels.len()) != keys.len() {
                    return Err("immutable input label arity differs from its descriptor".into());
                }
                let population = Self::rebuild_label_map(&keys, &entry.label);
                observed_populations.insert(population.clone());
                if population != *group {
                    continue;
                }
                let state = reconstruct_exact_agg(&entry.sketch_type_name, &entry.sketch_bytes)
                    .ok_or("immutable input accumulator cannot be decoded")?;
                if windows
                    .insert((record.start_ts, record.end_ts), Arc::from(state))
                    .is_some()
                {
                    // No last-writer-wins or implicit merge at a frozen source
                    // boundary: the producer must publish one complete state.
                    return Err("immutable input has multiple states for one window".into());
                }
            }
        }
        if windows.keys().copied().collect::<BTreeSet<_>>() != *expected_windows {
            return Err("immutable input window coverage is missing, expired, or ambiguous".into());
        }
        self.validate_routed_catalog_generation(Some(generation.as_ref()))?;
        Ok(FrozenExactWindows {
            stored_output_reference,
            storage_handle: sid,
            definition,
            generation: Arc::clone(generation),
            group: group.clone(),
            windows,
            singleton_population_complete: singleton_population_complete
                && observed_populations.len() == 1,
        })
    }
}

impl SketchStore {
    fn validate_frozen_cohort_bindings<'a>(
        &self,
        config: &asap_types::PrecomputeMaterialization,
        sources: &'a [FrozenExactWindows],
        instances: &HashMap<u64, SdsBinding>,
    ) -> Result<&'a Arc<CatalogGeneration>, String> {
        let generation = &sources
            .first()
            .ok_or("immutable publication input cohort is empty")?
            .generation;
        let definitions = sources.iter().map(|source| source.definition).collect();
        if config
            .derived_input
            .as_ref()
            .is_none_or(|input| input.inputs != definitions)
        {
            return Err(
                "immutable publication input definitions differ from installed identity".into(),
            );
        }
        let mut populations = BTreeSet::new();
        for source in sources {
            if &source.generation != generation
                || !populations.insert((source.definition, source.storage_handle, &source.group))
            {
                return Err(
                    "immutable publication input cohort has mixed generations or duplicates".into(),
                );
            }
            let binding = instances
                .get(&source.storage_handle)
                .ok_or("immutable source was removed")?;
            if binding.stored_output_reference != Some(source.stored_output_reference.clone())
                || binding.metadata.policy_fp != source.definition.fingerprint()
                || binding.catalog_generation.as_deref() != Some(generation.as_ref())
                || !binding.metadata.is_writable()
            {
                return Err("immutable source identity or lifetime changed".into());
            }
        }
        self.validate_routed_catalog_generation(Some(generation.as_ref()))?;
        Ok(generation)
    }

    fn validate_complete_raw_cohort_locked(
        &self,
        cohort: &CompleteRawMaintenanceCohort,
        instances: &HashMap<u64, SdsBinding>,
        finite_complete: bool,
    ) -> Result<(), String> {
        if !finite_complete {
            return Err("complete raw publication requires the original finite closure".into());
        }
        let mut supplied = BTreeMap::<StoredOutputId, BTreeSet<u64>>::new();
        for input in cohort.inputs() {
            supplied
                .entry(input.definition)
                .or_default()
                .insert(input.storage_handle);
        }
        for (definition, supplied_sids) in supplied {
            let mut current = self.durable_maintenance_population_ids(definition)?;
            current.extend(
                instances
                    .iter()
                    .filter(|(_, binding)| {
                        binding.metadata.policy_fp == definition.fingerprint()
                            && binding.stored_output_reference
                                == self.descriptors.stored_output_reference(definition)
                            && binding.catalog_generation.as_deref()
                                == cohort
                                    .inputs()
                                    .first()
                                    .map(|input| input.generation.as_ref())
                    })
                    .map(|(sid, _)| *sid),
            );
            if current != supplied_sids {
                return Err("complete raw population changed before publication".into());
            }
        }
        // The caller holds admission through commit. A finite closure rejects
        // all raw additive entry points, so the stored group payload set cannot
        // grow between inventory capture and this transaction.
        Ok(())
    }

    pub(crate) fn recover_frozen_maintenance_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        sources: &[FrozenExactWindows],
        digest: [u8; 32],
        window: (u64, u64),
    ) -> Result<bool, String> {
        self.recover_frozen_output(
            sid,
            config,
            FrozenPublicationInputs::Requested(sources),
            digest,
            window,
        )
    }

    pub(crate) fn recover_complete_raw_maintenance_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        cohort: &CompleteRawMaintenanceCohort,
        digest: [u8; 32],
        window: (u64, u64),
    ) -> Result<bool, String> {
        self.recover_frozen_output(
            sid,
            config,
            FrozenPublicationInputs::Complete(cohort),
            digest,
            window,
        )
    }

    fn recover_frozen_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        proof: FrozenPublicationInputs<'_>,
        digest: [u8; 32],
        window: (u64, u64),
    ) -> Result<bool, String> {
        let sources = proof.inputs();
        let _admission = self
            .admission
            .read()
            .map_err(|_| "admission registry poisoned")?;
        let instances = self
            .instances
            .read()
            .map_err(|_| "instance registry poisoned")?;
        let generation = self.validate_frozen_cohort_bindings(config, sources, &instances)?;
        if let FrozenPublicationInputs::Complete(cohort) = &proof {
            if cohort.window != window {
                return Err("complete raw cohort window differs from output".into());
            }
            self.validate_complete_raw_cohort_locked(
                cohort,
                &instances,
                _admission.is_finite_complete(),
            )?;
        }

        let Some(binding) = instances.get(&sid) else {
            return Ok(false);
        };
        if binding.metadata.policy_fp != config.policy_fingerprint()
            || binding.catalog_generation.as_deref() != Some(generation.as_ref())
            || !binding.metadata.is_writable()
        {
            return Err("immutable output identity or lifetime changed".into());
        }
        let record = self
            .metadata_record_without_completion(binding)
            .ok_or("immutable output has no catalog metadata")?;
        let publisher = self
            .immutable_publisher
            .read()
            .map_err(|_| "publisher registry poisoned")?
            .upgrade()
            .ok_or("immutable publication requires active persistence")?;
        let _mutation = self.begin_state_mutation();
        let mut completed = self
            .completed_windows
            .write()
            .map_err(|_| "completion registry poisoned")?;
        if publisher
            .resume_matching_immutable_window(&record, digest, window.0, window.1)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            completed
                .entry(sid)
                .and_modify(|end| *end = (*end).max(window.1))
                .or_insert(window.1);
            return Ok(true);
        }
        let found = publisher
            .lookup_immutable_window(&record, digest, window.0, window.1)
            .map_err(|error| error.to_string())?
            .is_some();
        if found {
            completed
                .entry(sid)
                .and_modify(|end| *end = (*end).max(window.1))
                .or_insert(window.1);
        }
        Ok(found)
    }

    /// Publish a derived result without passing it through additive hot-state
    /// append. The existing part reservation commits it once and seals it.
    pub(crate) fn publish_frozen_maintenance_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        state: &dyn AggregateCore,
        sources: &[FrozenExactWindows],
        input_digest: [u8; 32],
    ) -> Result<bool, String> {
        self.publish_frozen_output(
            sid,
            config,
            output,
            state,
            FrozenPublicationInputs::Requested(sources),
            input_digest,
        )
    }

    pub(crate) fn publish_complete_raw_maintenance_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        state: &dyn AggregateCore,
        cohort: &CompleteRawMaintenanceCohort,
        input_digest: [u8; 32],
    ) -> Result<bool, String> {
        self.publish_frozen_output(
            sid,
            config,
            output,
            state,
            FrozenPublicationInputs::Complete(cohort),
            input_digest,
        )
    }

    fn publish_frozen_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        state: &dyn AggregateCore,
        proof: FrozenPublicationInputs<'_>,
        input_digest: [u8; 32],
    ) -> Result<bool, String> {
        let sources = proof.inputs();
        use persistence::source::{EpochSnapshot, EpochSnapshotEntry};
        // One guard excludes source retirement through target registration
        // and commit; admission excludes catalog transitions in the same span.
        let _admission = self
            .admission
            .read()
            .map_err(|_| "admission registry poisoned")?;
        let mut instances = self
            .instances
            .write()
            .map_err(|_| "instance registry poisoned")?;
        let generation = self.validate_frozen_cohort_bindings(config, sources, &instances)?;
        if let FrozenPublicationInputs::Complete(cohort) = &proof {
            if cohort.window != (output.start_timestamp, output.end_timestamp) {
                return Err("complete raw cohort window differs from output".into());
            }
            self.validate_complete_raw_cohort_locked(
                cohort,
                &instances,
                _admission.is_finite_complete(),
            )?;
        }

        if output.policy_fp != config.policy_fingerprint()
            || output.catalog_generation.as_deref() != Some(generation.as_ref())
        {
            return Err("derived publication differs from its installed input identity".into());
        }
        let labels = self
            .register_precompute_output_with_instances(sid, config, output, &mut instances)
            .ok_or("derived output registration failed")?;
        let _mutation = self.begin_state_mutation();
        let binding = instances.get(&sid).ok_or("derived output was removed")?;
        if !binding.metadata.is_writable()
            || binding.metadata.policy_fp != output.policy_fp
            || binding.catalog_generation.as_deref() != Some(generation.as_ref())
        {
            return Err("derived publication physical lifetime changed".into());
        }
        let mut completed = self
            .completed_windows
            .write()
            .map_err(|_| "completion registry poisoned")?;
        let record = self
            .metadata_record_without_completion(binding)
            .ok_or("derived publication lacks catalog metadata")?;
        let publisher = self
            .immutable_publisher
            .read()
            .map_err(|_| "publisher registry poisoned")?
            .upgrade()
            .ok_or("immutable publication requires active persistence")?;
        let bytes = state.serialize_to_bytes();
        let snapshot = EpochSnapshot {
            agg_id: sid,
            epoch_id: 0,
            min_ts: output.start_timestamp,
            max_ts: output.end_timestamp,
            approx_bytes: bytes.len(),
            entries: vec![EpochSnapshotEntry {
                start_ts: output.start_timestamp,
                end_ts: output.end_timestamp,
                label: Some(crate::storage_engines::types::KeyByLabelValues {
                    labels: labels.into_values().collect(),
                }),
                sketch_type_name: state.type_name().to_string(),
                encoding_tag: match binding.metadata.agg_kind {
                    AggKind::Sketch { .. } => encoding_to_tag(SketchEncoding::MsgpackFull),
                    AggKind::ExactAgg { .. } => 0,
                },
                sketch_bytes: bytes,
            }],
        };
        // Another executor may have completed the same input after our first
        // lookup. Reuse its durable payload instead of comparing a newly built,
        // potentially randomized sketch byte representation.
        if publisher
            .lookup_immutable_window(
                &record,
                input_digest,
                output.start_timestamp,
                output.end_timestamp,
            )
            .map_err(|error| error.to_string())?
            .is_some()
        {
            completed
                .entry(sid)
                .and_modify(|end| *end = (*end).max(output.end_timestamp))
                .or_insert(output.end_timestamp);
            return Ok(false);
        }
        let published = publisher
            .publish_immutable_window(&record, input_digest, &snapshot)
            .map_err(|error| error.to_string())?;
        completed
            .entry(sid)
            .and_modify(|end| *end = (*end).max(output.end_timestamp))
            .or_insert(output.end_timestamp);
        if !published.already_published {
            crate::precompute_engine::metrics::record_materialized_outputs(1);
        }
        Ok(!published.already_published)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::types::PrecomputedOutput;
    use asap_physical_operators::summary_kernels::SumAccumulator;
    use asap_types::traits::SerializableToSink;

    #[test]
    fn complete_population_keeps_every_sid_and_rejects_missing_live_binding() {
        // Multiple SIDs are inventory entries, never an implicit singleton;
        // removing a live binding cannot hide its retained durable population.
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let mut first = plan.precompute_plan.materializations[0].clone();
        first.aggregation_type = asap_types::AggregationType::Sum;
        first.aggregation_sub_type = "sum".into();
        first.grouping_labels = ["instance".to_string()].into_iter().collect();
        let mut target = first.clone();
        target.derived_input = Some(asap_types::derived_input::DerivedInputIdentity {
            inputs: BTreeSet::from([first.policy_fingerprint().into()]),
            program_sha256: "0".repeat(64),
        });
        let configs = [first.clone(), first, target];
        let catalog =
            asap_types::summary_catalog::SummaryCatalog::from_materializations(1, 1, &configs)
                .unwrap();
        let store = Arc::new(SketchStore::new());
        store
            .install_summary_catalog(Arc::new(catalog.clone()))
            .unwrap();
        let generation = store.active_catalog_generation().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut config = persistence::config::SketchStorePersistenceConfig::with_memory_limit(
            1 << 24,
            directory.path().to_path_buf(),
        );
        config.delete_older_than_ms = None;
        config.hot_window_ms = None;
        let mut persistence = store.start_persistence(config).unwrap();
        for (index, config) in configs.iter().take(2).enumerate() {
            let definition = config.policy_fingerprint().into();
            let population = BTreeMap::from([("instance".to_string(), index.to_string())]);
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                stored_output_id: definition,
                time_range: HalfOpenTimeRange {
                    start_ms: 0,
                    end_ms: 1000,
                },
                group_values: population.clone(),
            };
            let revision = store
                .admit_summary_updates(&generation, BTreeSet::from([coordinate.clone()]))
                .unwrap();
            let mut output = PrecomputedOutput::new(0, 1000, None, config.policy_fingerprint());
            output.catalog_generation = Some(Arc::clone(&generation));
            output.population_labels = Some(population);
            let mut sum = SumAccumulator::new();
            sum.update(5.0 + index as f64);
            let sid = 900 + index as u64;
            store
                .publish_admitted_summary_update(
                    &generation,
                    &coordinate,
                    revision,
                    revision,
                    2000,
                    |writer| writer.ingest_precompute_with_series_id(sid, config, &output, &sum),
                )
                .unwrap();
        }
        let deadline =
            crate::tests::test_utilities::timing::deadline(std::time::Duration::from_secs(5));
        while !store.seal_finite_summary_input(&generation).unwrap() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let definition = configs[0].policy_fingerprint().into();
        let parts = persistence.manifest.live_parts().len();
        let metadata = persistence
            .flusher
            .metadata_store()
            .load_strict()
            .unwrap()
            .len();
        let inventory = store
            .complete_raw_maintenance_population(definition, &generation)
            .unwrap();
        assert_eq!(
            inventory.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([900, 901])
        );
        assert!(inventory.values().all(|groups| groups.len() == 1));
        assert!(store
            .completed_maintenance_coordinates(definition, &generation)
            .is_err());
        store.instances.write().unwrap().remove(&901);
        assert!(store
            .complete_raw_maintenance_population(definition, &generation)
            .is_err());
        assert_eq!(persistence.manifest.live_parts().len(), parts);
        assert_eq!(
            persistence
                .flusher
                .metadata_store()
                .load_strict()
                .unwrap()
                .len(),
            metadata
        );
        persistence.shutdown();
    }

    #[test]
    fn cohort_requires_every_durable_source_in_one_catalog_generation() {
        // Neither a missing second window nor a new catalog may yield a
        // partially acquired cohort, even when the first source is complete.
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let mut first = plan.precompute_plan.materializations[0].clone();
        first.aggregation_type = asap_types::AggregationType::Sum;
        first.aggregation_sub_type = "sum".into();
        first.grouping_labels = std::iter::empty::<String>().collect();
        let mut second = first.clone();
        second.metric = "cohort_second".into();
        let mut target = first.clone();
        target.derived_input = Some(asap_types::derived_input::DerivedInputIdentity {
            inputs: BTreeSet::from([
                first.policy_fingerprint().into(),
                second.policy_fingerprint().into(),
            ]),
            program_sha256: "0".repeat(64),
        });
        let configs = [first, second, target];
        let catalog =
            asap_types::summary_catalog::SummaryCatalog::from_materializations(1, 1, &configs)
                .unwrap();
        let mut store = Arc::new(SketchStore::new());
        store
            .install_summary_catalog(Arc::new(catalog.clone()))
            .unwrap();
        let generation = store.active_catalog_generation().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut config = persistence::config::SketchStorePersistenceConfig::with_memory_limit(
            1 << 24,
            directory.path().to_path_buf(),
        );
        config.delete_older_than_ms = None;
        config.hot_window_ms = None;
        let mut persistence = store.start_persistence(config).unwrap();
        let mut requests = Vec::new();
        for (index, config) in configs.iter().take(2).enumerate() {
            let definition = config.policy_fingerprint().into();
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                stored_output_id: definition,
                time_range: HalfOpenTimeRange {
                    start_ms: 0,
                    end_ms: 1000,
                },
                group_values: BTreeMap::new(),
            };
            let revision = store
                .admit_summary_updates(&generation, BTreeSet::from([coordinate.clone()]))
                .unwrap();
            let mut output = PrecomputedOutput::new(0, 1000, None, config.policy_fingerprint());
            output.catalog_generation = Some(Arc::clone(&generation));
            let mut sum = SumAccumulator::new();
            sum.update(5.0 + index as f64);
            let sid = 900 + index as u64;
            store
                .publish_admitted_summary_update(
                    &generation,
                    &coordinate,
                    revision,
                    revision,
                    2000,
                    |writer| writer.ingest_precompute_with_series_id(sid, config, &output, &sum),
                )
                .unwrap();
            requests.push((
                sid,
                definition,
                BTreeSet::from([(0, 1000)]),
                BTreeMap::new(),
            ));
        }
        // Only the first population has the next window: completeness must
        // reject the partial cohort even after all writes are durably sealed.
        let extra_coordinate = asap_types::sds::SummaryInstanceCoordinates {
            stored_output_id: configs[0].policy_fingerprint().into(),
            time_range: HalfOpenTimeRange {
                start_ms: 1000,
                end_ms: 2000,
            },
            group_values: BTreeMap::new(),
        };
        let extra_revision = store
            .admit_summary_updates(&generation, BTreeSet::from([extra_coordinate.clone()]))
            .unwrap();
        let mut extra_window =
            PrecomputedOutput::new(1000, 2000, None, configs[0].policy_fingerprint());
        extra_window.catalog_generation = Some(Arc::clone(&generation));
        let mut extra_sum = SumAccumulator::new();
        extra_sum.update(99.0);
        store
            .publish_admitted_summary_update(
                &generation,
                &extra_coordinate,
                extra_revision,
                extra_revision,
                3000,
                |writer| {
                    writer.ingest_precompute_with_series_id(
                        900,
                        &configs[0],
                        &extra_window,
                        &extra_sum,
                    )
                },
            )
            .unwrap();
        let deadline =
            crate::tests::test_utilities::timing::deadline(std::time::Duration::from_secs(5));
        while !store.seal_finite_summary_input(&generation).unwrap() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let definitions = requests.iter().map(|request| request.1).collect();
        let parts = persistence.manifest.live_parts().len();
        let complete = store
            .read_complete_raw_maintenance_cohort(&generation, &definitions, (0, 1000))
            .unwrap();
        assert_eq!(complete.inputs().len(), 2);
        assert!(store
            .read_complete_raw_maintenance_cohort(&generation, &definitions, (1000, 2000))
            .is_err());
        let cohort = store
            .read_frozen_exact_cohort(&generation, &definitions, &requests)
            .unwrap();
        assert_eq!(cohort.len(), 2);
        assert!(cohort.iter().all(|input| input.generation == generation));
        requests[1].2.insert((1000, 2000));
        assert!(store
            .read_frozen_exact_cohort(&generation, &definitions, &requests)
            .is_err());
        assert_eq!(persistence.manifest.live_parts().len(), parts);
        let target = &configs[2];
        let mut output = PrecomputedOutput::new(0, 1000, None, target.policy_fingerprint());
        output.catalog_generation = Some(Arc::clone(&generation));
        let mut sum = SumAccumulator::new();
        sum.update(11.0);
        assert!(store
            .publish_complete_raw_maintenance_output(
                902, target, &output, &sum, &complete, [42; 32]
            )
            .unwrap());
        assert!(store
            .recover_complete_raw_maintenance_output(902, target, &complete, [42; 32], (0, 1000))
            .unwrap());
        let published_parts = persistence.manifest.live_parts().len();
        assert!(store
            .recover_complete_raw_maintenance_output(902, target, &complete, [42; 32], (1000, 2000))
            .is_err());
        // Reconstruct the proof from durable sources after a real restart;
        // recovery must reuse the committed output without rebuilding it.
        persistence.shutdown();
        store = Arc::new(SketchStore::new());
        store
            .install_summary_catalog(Arc::new(catalog.clone()))
            .unwrap();
        let mut restart_config =
            persistence::config::SketchStorePersistenceConfig::with_memory_limit(
                1 << 24,
                directory.path().to_path_buf(),
            );
        restart_config.delete_older_than_ms = None;
        restart_config.hot_window_ms = None;
        persistence = store.start_persistence(restart_config).unwrap();
        let complete = store
            .read_complete_raw_maintenance_cohort(&generation, &definitions, (0, 1000))
            .unwrap();
        assert!(store
            .recover_complete_raw_maintenance_output(902, target, &complete, [42; 32], (0, 1000))
            .unwrap());
        assert_eq!(persistence.manifest.live_parts().len(), published_parts);
        // A retained proof cannot authorize a subset after even an empty raw
        // lifetime is registered. No new target reservation may be created.
        let mut extra = PrecomputedOutput::new(0, 1000, None, configs[0].policy_fingerprint());
        extra.catalog_generation = Some(Arc::clone(&generation));
        assert!(store
            .register_precompute_output(904, &configs[0], &extra)
            .is_some());
        assert!(store
            .publish_complete_raw_maintenance_output(
                905, target, &output, &sum, &complete, [42; 32]
            )
            .is_err());
        assert!(!store.instances.read().unwrap().contains_key(&905));
        assert_eq!(persistence.manifest.live_parts().len(), published_parts);
        assert!(!persistence
            .flusher
            .metadata_store()
            .load_strict()
            .unwrap()
            .iter()
            .any(|record| record.storage_handle == 905));
        store.force_expire(901).unwrap();
        assert!(store
            .publish_frozen_maintenance_output(903, target, &output, &sum, &cohort, [42; 32])
            .is_err());
        assert!(store
            .recover_frozen_maintenance_output(902, target, &cohort, [42; 32], (0, 1000))
            .is_err());
        assert!(!store.instances.read().unwrap().contains_key(&903));
        assert_eq!(persistence.manifest.live_parts().len(), published_parts);
        assert!(!store
            .persistence_metadata
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .load_strict()
            .unwrap()
            .iter()
            .any(|record| record.storage_handle == 903));
        let mut next = catalog;
        next.plan_version += 1;
        store.install_summary_catalog(Arc::new(next)).unwrap();
        requests[1].2.remove(&(1000, 2000));
        assert!(store
            .read_frozen_exact_cohort(&generation, &definitions, &requests)
            .is_err());
        persistence.shutdown();
    }

    #[test]
    fn complete_populations_publish_once_and_reject_foreign_generation() {
        let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .unwrap();
        let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
        entry["query"] = "quantile(0.9, sum_over_time(immutable_value[1m]))".into();
        entry["demand"]["fixed_interval_at"]["interval"] = 60_000.into();
        entry["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
        fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_value(fixture).unwrap();
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let source = plan
            .precompute_plan
            .materializations
            .iter()
            .find(|config| config.derived_input.is_none())
            .unwrap();
        let target = plan
            .precompute_plan
            .materializations
            .iter()
            .find(|config| config.derived_input.is_some())
            .unwrap();
        let store = Arc::new(SketchStore::new());
        store
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        let generation = store.active_catalog_generation().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut config = persistence::config::SketchStorePersistenceConfig::with_memory_limit(
            1 << 24,
            directory.path().to_path_buf(),
        );
        config.delete_older_than_ms = None;
        config.hot_window_ms = None;
        let mut persistence = store.start_persistence(config).unwrap();
        for (instance, value) in [("a", 5.0), ("b", 15.0)] {
            let population = BTreeMap::from([("instance".to_string(), instance.to_string())]);
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                stored_output_id: source.policy_fingerprint().into(),
                time_range: HalfOpenTimeRange {
                    start_ms: 0,
                    end_ms: 60_000,
                },
                group_values: population.clone(),
            };
            let revision = store
                .admit_summary_updates(&generation, BTreeSet::from([coordinate.clone()]))
                .unwrap();
            let mut output = PrecomputedOutput::new(0, 60_000, None, source.policy_fingerprint());
            output.population_labels = Some(population);
            output.catalog_generation = Some(Arc::clone(&generation));
            let mut sum = SumAccumulator::new();
            sum.update(value);
            store
                .publish_admitted_summary_update(
                    &generation,
                    &coordinate,
                    revision,
                    revision,
                    120_000,
                    |writer| writer.ingest_precompute_with_series_id(700, source, &output, &sum),
                )
                .unwrap();
        }
        assert!(store
            .complete_raw_maintenance_population(source.policy_fingerprint().into(), &generation)
            .is_err());
        let deadline =
            crate::tests::test_utilities::timing::deadline(std::time::Duration::from_secs(5));
        while !store.seal_finite_summary_input(&generation).unwrap() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let source_id = source.policy_fingerprint().into();
        let coordinates = store
            .completed_maintenance_coordinates(source_id, &generation)
            .unwrap();
        assert_eq!(coordinates.len(), 1);
        assert_eq!(coordinates[&700].len(), 2);
        assert_eq!(
            store
                .complete_raw_maintenance_population(source_id, &generation)
                .unwrap(),
            coordinates
        );

        let group = BTreeMap::from([("instance".into(), "a".into())]);
        let frozen = store
            .read_frozen_exact_windows(
                700,
                source_id,
                &generation,
                &BTreeSet::from([(0, 60_000)]),
                &group,
            )
            .unwrap();
        assert!(!frozen.singleton_population_complete);
        // The global reduction must consume both populations in one read set.
        let request = |name: &str| {
            (
                700,
                source_id,
                BTreeSet::from([(0, 60_000)]),
                BTreeMap::from([("instance".to_string(), name.to_string())]),
            )
        };
        let cohort = store
            .read_frozen_exact_cohort(
                &generation,
                &BTreeSet::from([source_id]),
                &[request("b"), request("a")],
            )
            .unwrap();
        assert_eq!(cohort.len(), 2);
        assert_eq!(cohort[0].group["instance"], "a");
        assert_eq!(cohort[1].group["instance"], "b");
        assert!(store
            .read_frozen_exact_cohort(
                &generation,
                &BTreeSet::from([source_id]),
                &[request("a"), request("a")]
            )
            .is_err());
        assert!(store
            .read_frozen_exact_cohort(
                &generation,
                &BTreeSet::from([source_id, target.policy_fingerprint().into()]),
                &[request("a")]
            )
            .is_err());
        assert!(store
            .read_frozen_exact_cohort(
                &generation,
                &BTreeSet::from([source_id]),
                &[request("a"), request("absent")]
            )
            .is_err());
        let parts_before = persistence.manifest.live_parts().len();
        let resolver = crate::drivers::ingest::series_resolver::SeriesIdResolver::new();
        crate::precompute_engine::maintenance_runtime::execute_finite_maintenance(
            &store,
            &resolver,
            &plan.precompute_plan,
        )
        .unwrap();
        let targets = store.series_ids_for_policy(target.policy_fingerprint());
        assert_eq!(targets.len(), 1);
        let target_sid = targets[0];
        let published = store
            .completed_maintenance_coordinates(target.policy_fingerprint().into(), &generation)
            .unwrap();
        assert_eq!(
            published,
            BTreeMap::from([(
                target_sid,
                BTreeMap::from([(BTreeMap::new(), BTreeSet::from([(0, 60_000)]))]),
            )])
        );
        let assert_complete_output = |store: &SketchStore| {
            use crate::storage_engines::sketch_db::data::SketchEncoding;
            use asap_physical_operators::summary_kernels::DDSketchAccumulator;
            let rows = store.query_range(target_sid, 0, 60_000);
            assert_eq!(rows.len(), 1);
            assert!(rows[0].series_label_values.is_empty());
            assert_eq!(rows[0].samples.len(), 1);
            let frames = &rows[0].samples[&60_000];
            assert_eq!(frames.len(), 1);
            let sketch = match frames[0].encoding {
                SketchEncoding::MsgpackFull => {
                    DDSketchAccumulator::from_msgpack_bytes(&frames[0].bytes).unwrap()
                }
                SketchEncoding::ProtoFull => {
                    DDSketchAccumulator::from_sketchlib_proto_bytes(&frames[0].bytes).unwrap()
                }
                other => panic!("unexpected derived encoding: {other:?}"),
            };
            assert_eq!(sketch.inner.total_count(), 2);
            for (q, expected) in [(0.0, 5.0), (1.0, 15.0)] {
                let actual = sketch.inner.quantile(q).unwrap();
                assert!((actual - expected).abs() <= expected * sketch.inner.wire_alpha());
            }
            frames[0].bytes.clone()
        };
        let bytes = assert_complete_output(&store);
        assert_eq!(persistence.manifest.live_parts().len(), parts_before + 1);
        crate::precompute_engine::maintenance_runtime::execute_finite_maintenance(
            &store,
            &resolver,
            &plan.precompute_plan,
        )
        .unwrap();
        assert_eq!(assert_complete_output(&store), bytes);
        assert_eq!(persistence.manifest.live_parts().len(), parts_before + 1);
        persistence.shutdown();

        // Replaying the same generation after recovery must reuse the output.
        let recovered = Arc::new(SketchStore::new());
        recovered
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        let mut recovery_config =
            persistence::config::SketchStorePersistenceConfig::with_memory_limit(
                1 << 24,
                directory.path().to_path_buf(),
            );
        recovery_config.delete_older_than_ms = None;
        recovery_config.hot_window_ms = None;
        let mut recovery = recovered.start_persistence(recovery_config).unwrap();
        crate::precompute_engine::maintenance_runtime::execute_finite_maintenance(
            &recovered,
            &resolver,
            &plan.precompute_plan,
        )
        .unwrap();
        assert_eq!(assert_complete_output(&recovered), bytes);
        assert_eq!(recovery.manifest.live_parts().len(), parts_before + 1);
        recovery.shutdown();

        // A catalog transition deliberately leaves old-generation payload
        // unbound on restart. It must still count against singleton proof.
        let mut next_catalog = plan.summary_catalog.clone();
        next_catalog.plan_version += 1;
        let restarted = Arc::new(SketchStore::new());
        restarted
            .install_summary_catalog(Arc::new(next_catalog))
            .unwrap();
        let next_generation = restarted.active_catalog_generation().unwrap();
        let mut restart_config =
            persistence::config::SketchStorePersistenceConfig::with_memory_limit(
                1 << 24,
                directory.path().to_path_buf(),
            );
        restart_config.delete_older_than_ms = None;
        restart_config.hot_window_ms = None;
        let mut restored = restarted.start_persistence(restart_config).unwrap();
        let population = BTreeMap::from([("instance".to_string(), "new".to_string())]);
        let coordinate = asap_types::sds::SummaryInstanceCoordinates {
            stored_output_id: source_id,
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 60_000,
            },
            group_values: population.clone(),
        };
        let revision = restarted
            .admit_summary_updates(&next_generation, BTreeSet::from([coordinate.clone()]))
            .unwrap();
        let mut output = PrecomputedOutput::new(0, 60_000, None, source.policy_fingerprint());
        output.population_labels = Some(population.clone());
        output.catalog_generation = Some(Arc::clone(&next_generation));
        let mut sum = SumAccumulator::new();
        sum.update(9.0);
        restarted
            .publish_admitted_summary_update(
                &next_generation,
                &coordinate,
                revision,
                revision,
                120_000,
                |writer| writer.ingest_precompute_with_series_id(702, source, &output, &sum),
            )
            .unwrap();
        let deadline =
            crate::tests::test_utilities::timing::deadline(std::time::Duration::from_secs(5));
        while !restarted
            .seal_finite_summary_input(&next_generation)
            .unwrap()
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            restarted
                .durable_maintenance_population_ids(source_id)
                .unwrap(),
            BTreeSet::from([700, 702])
        );
        assert!(
            !restarted
                .read_frozen_exact_windows(
                    702,
                    source_id,
                    &next_generation,
                    &BTreeSet::from([(0, 60_000)]),
                    &population
                )
                .unwrap()
                .singleton_population_complete
        );
        let mut next_plan = plan.precompute_plan.clone();
        next_plan.summary_catalog = Some(next_generation.as_ref().clone());
        let parts_before = restored.manifest.live_parts().len();
        let durable_records = || {
            restored
                .flusher
                .metadata_store()
                .load_strict()
                .unwrap()
                .into_iter()
                .map(|record| (record.storage_handle, serde_json::to_value(record).unwrap()))
                .collect::<BTreeMap<_, _>>()
        };
        let records_before = durable_records();
        assert!(
            crate::precompute_engine::maintenance_runtime::execute_finite_maintenance(
                &restarted, &resolver, &next_plan
            )
            .is_err()
        );
        assert!(restarted
            .series_ids_for_policy(target.policy_fingerprint())
            .is_empty());
        assert_eq!(restored.manifest.live_parts().len(), parts_before);
        assert_eq!(durable_records(), records_before);
        restored.shutdown();
    }

    #[test]
    fn committed_recovery_restores_live_seal_and_rejects_additive_derived_writes() {
        // A durable sidecar may survive an I/O error before the caller updates
        // its live seal. Recovery must repair admission, not only return a hit.
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let mut source_config = plan.precompute_plan.materializations[0].clone();
        source_config.aggregation_type = asap_types::AggregationType::Sum;
        source_config.aggregation_sub_type = "sum".into();
        let source_id = source_config.policy_fingerprint().into();
        let mut target = source_config.clone();
        target.derived_input = Some(asap_types::derived_input::DerivedInputIdentity {
            inputs: BTreeSet::from([source_id]),
            program_sha256: "0".repeat(64),
        });
        let catalog = asap_types::summary_catalog::SummaryCatalog::from_materializations(
            1,
            1,
            &[source_config.clone(), target.clone()],
        )
        .unwrap();
        let store = Arc::new(SketchStore::new());
        store.install_summary_catalog(Arc::new(catalog)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut config = persistence::config::SketchStorePersistenceConfig::with_memory_limit(
            1 << 24,
            directory.path().to_path_buf(),
        );
        config.delete_older_than_ms = None;
        config.hot_window_ms = None;
        let mut persistence = store.start_persistence(config).unwrap();
        let generation = store.active_catalog_generation().unwrap();
        let mut output = PrecomputedOutput::new(0, 1000, None, target.policy_fingerprint());
        output.catalog_generation = Some(Arc::clone(&generation));
        let mut source_output = output.clone();
        source_output.policy_fp = source_config.policy_fingerprint();
        store
            .register_precompute_output(600, &source_config, &source_output)
            .unwrap();
        store
            .register_precompute_output(601, &target, &output)
            .unwrap();
        let record = store
            .metadata_record(&store.instances.read().unwrap()[&601])
            .unwrap();
        let state = SumAccumulator::new();
        let snapshot = persistence::source::EpochSnapshot {
            agg_id: 601,
            epoch_id: 0,
            min_ts: 0,
            max_ts: 1000,
            approx_bytes: 0,
            entries: vec![persistence::source::EpochSnapshotEntry {
                start_ts: 0,
                end_ts: 1000,
                label: None,
                sketch_type_name: state.type_name().into(),
                encoding_tag: 0,
                sketch_bytes: state.serialize_to_bytes(),
            }],
        };
        persistence
            .flusher
            .publish_immutable_window(&record, [7; 32], &snapshot)
            .unwrap();
        assert!(!store.completed_windows.read().unwrap().contains_key(&601));
        let input = FrozenExactWindows {
            stored_output_reference: store
                .descriptors
                .stored_output_reference(source_id)
                .unwrap(),
            storage_handle: 600,
            definition: source_id,
            generation,
            group: BTreeMap::new(),
            windows: BTreeMap::new(),
            singleton_population_complete: false,
        };
        assert!(store
            .recover_frozen_maintenance_output(
                601,
                &target,
                std::slice::from_ref(&input),
                [7; 32],
                (0, 1000)
            )
            .unwrap());
        assert_eq!(
            store.completed_windows.read().unwrap().get(&601),
            Some(&1000)
        );
        assert!(!store.append_precompute(
            601,
            BTreeMap::new(),
            (2000, 3000),
            Box::new(SumAccumulator::new())
        ));
        assert!(!store.append_sample(
            601,
            BTreeMap::new(),
            (2000, 3000),
            SketchSampleState {
                bytes: vec![],
                encoding: SketchEncoding::MsgpackFull,
            }
        ));
        persistence.shutdown();
    }
}
