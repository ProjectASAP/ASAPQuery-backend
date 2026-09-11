//! Immutable maintenance inputs read from the existing durable summary tier.
//!
//! Unlike query fallback helpers, this path must propagate corrupt/missing part
//! errors: silently omitting a source window would permanently corrupt an outer
//! summary. Completion is an admission barrier, not merely an emitted flag.
use super::*;
use crate::storage_engines::types::AggregateCore;

pub(crate) struct FrozenExactWindows {
    pub(crate) sid: u64,
    pub(crate) definition: SummaryDefinitionId,
    pub(crate) generation: Arc<CatalogGeneration>,
    pub(crate) group: BTreeMap<String, String>,
    pub(crate) windows: BTreeMap<(u64, u64), Arc<dyn AggregateCore>>,
}

impl SketchStore {
    pub(crate) fn read_frozen_exact_windows(
        &self,
        sid: u64,
        definition: SummaryDefinitionId,
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
        let keys = {
            let bindings = self
                .instances
                .read()
                .map_err(|_| "instance registry poisoned")?;
            let binding = bindings.get(&sid).ok_or("immutable input SID is absent")?;
            if binding.metadata.policy_fp != definition.fingerprint()
                || binding.catalog_generation.as_deref() != Some(generation.as_ref())
                || binding.metadata.status() == AggStatus::Expired
            {
                return Err("immutable input identity or lifetime differs".into());
            }
            binding.metadata.group_by_keys.clone()
        };
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
                if Self::rebuild_label_map(&keys, &entry.label) != *group {
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
            sid,
            definition,
            generation: Arc::clone(generation),
            group: group.clone(),
            windows,
        })
    }
}

impl SketchStore {
    pub(crate) fn recover_frozen_maintenance_output(
        &self,
        sid: u64,
        config: &asap_types::PrecomputeMaterialization,
        source: &FrozenExactWindows,
        digest: [u8; 32],
        window: (u64, u64),
    ) -> Result<bool, String> {
        self.validate_routed_catalog_generation(Some(source.generation.as_ref()))?;
        let instances = self
            .instances
            .read()
            .map_err(|_| "instance registry poisoned")?;
        let Some(binding) = instances.get(&sid) else {
            return Ok(false);
        };
        if binding.metadata.policy_fp != config.policy_fingerprint()
            || binding.catalog_generation.as_deref() != Some(source.generation.as_ref())
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
        source: &FrozenExactWindows,
        input_digest: [u8; 32],
    ) -> Result<bool, String> {
        use persistence::source::{EpochSnapshot, EpochSnapshotEntry};
        if config
            .derived_input
            .as_ref()
            .is_none_or(|derived| derived.inputs != BTreeSet::from([source.definition]))
            || output.policy_fp != config.policy_fingerprint()
            || output.catalog_generation.as_deref() != Some(source.generation.as_ref())
        {
            return Err("derived publication differs from its installed input identity".into());
        }
        let labels = self
            .register_precompute_output(sid, config, output)
            .ok_or("derived output registration failed")?;
        let _mutation = self.begin_state_mutation();
        let instances = self
            .instances
            .read()
            .map_err(|_| "instance registry poisoned")?;
        let source_binding = instances
            .get(&source.sid)
            .ok_or("immutable source was removed")?;
        let binding = instances.get(&sid).ok_or("derived output was removed")?;
        if source_binding.metadata.policy_fp != source.definition.fingerprint()
            || source_binding.catalog_generation.as_deref() != Some(source.generation.as_ref())
            || source_binding.metadata.status() == AggStatus::Expired
            || !binding.metadata.is_writable()
            || binding.metadata.policy_fp != output.policy_fp
            || binding.catalog_generation.as_deref() != Some(source.generation.as_ref())
        {
            return Err("derived publication physical lifetime changed".into());
        }
        self.validate_routed_catalog_generation(Some(source.generation.as_ref()))?;
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
                encoding_tag: 0,
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
    use crate::precompute_engine::operators::SumAccumulator;
    use crate::storage_engines::types::PrecomputedOutput;
    use asap_types::traits::SerializableToSink;

    #[test]
    fn committed_recovery_restores_live_seal_and_rejects_additive_derived_writes() {
        // A durable sidecar may survive an I/O error before the caller updates
        // its live seal. Recovery must repair admission, not only return a hit.
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let plan = snapshot.compile().unwrap();
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
            &[source_config, target.clone()],
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
            sid: 600,
            definition: source_id,
            generation,
            group: BTreeMap::new(),
            windows: BTreeMap::new(),
        };
        assert!(store
            .recover_frozen_maintenance_output(601, &target, &input, [7; 32], (0, 1000))
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
