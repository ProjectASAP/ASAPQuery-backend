//! Bound native summary snapshots use the existing immutable publication and
//! recovery path. Window snapshots are not additive hot-state updates.
use super::*;
use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use asap_physical_operators::{
    stored_state::native::{decode_batch, encode_batch},
    values::{Batch, Schema, Value},
    AggregateCore, SerializableToSink,
};

const NATIVE_OUTPUT_TYPE: &str = "NativePhysicalOutputV1";
const NATIVE_OUTPUT_TAG: u8 = persistence::part::encoding_tag::NATIVE_BATCH_V1;

/// Preserve execution failures across the storage boundary; missing state remains
/// distinguishable from a terminal resource failure.
#[derive(Debug, thiserror::Error)]
pub enum NativeReadError {
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Physical(#[from] asap_physical_operators::Error),
}
impl From<String> for NativeReadError {
    fn from(message: String) -> Self {
        Self::Unavailable(message)
    }
}
impl From<&'static str> for NativeReadError {
    fn from(message: &'static str) -> Self {
        Self::Unavailable(message.into())
    }
}

#[derive(Clone)]
struct NativeSummaryOutput {
    batch: Batch,
    bytes: Vec<u8>,
    kind: AggregationType,
}
impl NativeSummaryOutput {
    fn new(batch: Batch, max_bytes: usize) -> Result<Self, String> {
        let families = batch
            .schema()
            .fields
            .iter()
            .filter_map(|field| {
                (!matches!(
                    field.dtype,
                    planner_types::post_asap::SummaryFamilyType::Plain(_)
                ))
                .then_some(&field.dtype)
            })
            .collect::<Vec<_>>();
        let [family] = families.as_slice() else {
            return Err("native stored batch requires one summary column".into());
        };
        use planner_types::post_asap::{SketchAlgorithm, SummaryFamilyType};
        let schema_kind = match family {
            SummaryFamilyType::Sketch(sketch, _) => match sketch.algorithm() {
                SketchAlgorithm::CmsWithHeap => Some(AggregationType::CountMinSketchWithHeap),
                SketchAlgorithm::CountSketchWithHeap => Some(AggregationType::CountSketchWithHeap),
                _ => None,
            },
            SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _) => {
                Some(AggregationType::Sum)
            }
            _ => None,
        };
        let mut kind = schema_kind;
        for row in batch.rows() {
            let states = row
                .iter()
                .filter_map(|value| match value {
                    Value::Summary { state, .. } => Some(state),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let [state] = states.as_slice() else {
                return Err("native stored row requires one summary state".into());
            };
            let row_kind = state.get_accumulator_type();
            if kind.is_some_and(|kind| kind != row_kind) {
                return Err("native stored rows have different summary families".into());
            }
            kind = Some(row_kind);
        }
        let kind = kind.ok_or("empty native batch has no supported summary family")?;
        let bytes = encode_batch(&batch).map_err(|error| error.to_string())?;
        if bytes.len() > max_bytes || batch.bytes() > max_bytes {
            return Err("native summary exceeds publication/read budget".into());
        }
        Ok(Self { batch, bytes, kind })
    }
    fn validate_group(&self, group: &BTreeMap<String, String>) -> Result<(), String> {
        for (key, value) in group {
            let column = self
                .batch
                .schema()
                .fields
                .iter()
                .position(|field| &field.name == key)
                .ok_or("native output is missing its stored group key")?;
            if self
                .batch
                .rows()
                .iter()
                .any(|row| !matches!(&row[column], Value::Utf8(actual) if actual.as_ref() == value))
            {
                return Err("native output group differs from stored address".into());
            }
        }
        Ok(())
    }
}
impl SerializableToSink for NativeSummaryOutput {
    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }
    fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::json!({"format": NATIVE_OUTPUT_TYPE, "bytes": self.bytes})
    }
}
impl AggregateCore for NativeSummaryOutput {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }
    fn type_name(&self) -> &'static str {
        NATIVE_OUTPUT_TYPE
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn get_accumulator_type(&self) -> AggregationType {
        self.kind
    }
    fn get_keys(&self) -> Option<Vec<crate::storage_engines::types::KeyByLabelValues>> {
        None
    }
    fn approx_memory_bytes(&self) -> usize {
        self.bytes.len() + self.batch.bytes()
    }
    fn merge_with(
        &self,
        _: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        Err("native output snapshots require an explicit physical merge operator".into())
    }
    fn query_statistic(
        &self,
        _: asap_types::Statistic,
        _: &Option<crate::storage_engines::types::KeyByLabelValues>,
        _: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("native output readout requires the installed physical DAG".into())
    }
}

impl SketchStore {
    /// Publish a finalized native result only after the complete raw input
    /// cohort is durable. Existing publication fences prevent duplicate commits.
    pub fn publish_native_summary_output(
        &self,
        resolver: &SeriesIdResolver,
        config: &asap_types::PrecomputeMaterialization,
        output: &crate::storage_engines::types::PrecomputedOutput,
        batch: Batch,
        max_bytes: usize,
    ) -> Result<bool, String> {
        use sha2::{Digest, Sha256};
        let generation = output
            .catalog_generation
            .as_ref()
            .ok_or("native output requires a catalog generation")?;
        let program = config
            .derived_input
            .as_ref()
            .ok_or("native output requires installed input lineage")?;
        let window = (output.start_timestamp, output.end_timestamp);
        let cohort =
            self.read_complete_raw_maintenance_cohort(generation, &program.inputs, window)?;
        let (population_key, group) = build_attrs_fp_and_label_map(config, output)?;
        let state = NativeSummaryOutput::new(batch, max_bytes)?;
        let family = config.accumulator_spec().map_err(|e| e.to_string())?.family;
        if state
            .batch
            .schema()
            .fields
            .iter()
            .filter(|field| {
                !matches!(
                    field.dtype,
                    planner_types::post_asap::SummaryFamilyType::Plain(_)
                )
            })
            .any(|field| field.dtype != family)
        {
            return Err("native output schema differs from installed definition".into());
        }
        for value in state.batch.rows().iter().flatten() {
            if let Value::Summary { family: actual, .. } = value {
                if actual != &family {
                    return Err("native output family differs from installed definition".into());
                }
            }
        }
        state.validate_group(&group)?;
        let sid = self.resolve_output_storage_handle(
            resolver,
            config.policy_fingerprint().into(),
            &population_key,
            Some(generation),
        )?;
        let mut digest = Sha256::new();
        digest.update(serde_json::to_vec(&(program, window)).map_err(|e| e.to_string())?);
        for input in cohort.inputs() {
            digest.update(
                serde_json::to_vec(&(&input.stored_output_reference, &input.group))
                    .map_err(|e| e.to_string())?,
            );
            for (window, state) in &input.windows {
                digest.update(serde_json::to_vec(window).map_err(|e| e.to_string())?);
                let bytes = state.serialize_to_bytes();
                digest.update((bytes.len() as u64).to_le_bytes());
                digest.update(bytes);
            }
        }
        self.publish_complete_raw_maintenance_output(
            sid,
            config,
            output,
            &state,
            &cohort,
            digest.finalize().into(),
        )
    }

    /// Resolve the installed output/group prefix without minting a new handle,
    /// then read exactly the requested immutable window and validate its format.
    pub fn read_native_summary_output(
        &self,
        resolver: &SeriesIdResolver,
        address: &asap_types::sds::StoredSummaryKey,
        reference: &StoredOutputReference,
        expected_schema: Schema,
        max_bytes: usize,
    ) -> Result<Batch, NativeReadError> {
        address.validate().map_err(|e| e.to_string())?;
        let (catalog, generation) = self
            .descriptors
            .authoritative_snapshot()
            .ok_or("native read requires an installed catalog")?;
        if (address.plan_id, address.plan_version) != (generation.plan_id, generation.plan_version)
            || address.stored_output_id != reference.stored_output_id
            || catalog
                .output_reference(address.stored_output_id)
                .map_err(|e| e.to_string())?
                != *reference
        {
            return Err("native read differs from its installed output reference".into());
        }
        let identity = &catalog.outputs[&address.stored_output_id];
        let data = &catalog.data_descriptors[&identity.data_descriptor_id];
        let pairs: Vec<_> = address
            .population
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let population_key = crate::drivers::ingest::population_attrs_fingerprint(
            data.population_key_encoding,
            &pairs,
        )?;
        let identity = serde_json::to_string(&(address.plan_id, address.plan_version, reference))
            .map_err(|e| e.to_string())?;
        let sid = resolver
            .lookup("stored-output", &population_key, &identity)
            .ok_or("native stored output/group is unavailable")?;
        self.read_native_summary_handle(sid, address, reference, expected_schema, max_bytes)
    }

    /// A complete native batch is stored atomically under one global address.
    /// Logical grouping remains in the batch; reads never allocate a resolver ID.
    pub fn read_bound_native_summary(
        &self,
        address: &asap_types::sds::StoredSummaryKey,
        reference: &StoredOutputReference,
        expected_schema: Schema,
        max_bytes: usize,
    ) -> Result<Batch, NativeReadError> {
        if !address.population.is_empty() {
            return Err("native batch binding requires the complete stored output".into());
        }
        if let Some(batches) = &self.native_revision_batches {
            address.validate().map_err(|e| e.to_string())?;
            let generation = self
                .active_catalog_generation()
                .ok_or("native revision has no generation")?;
            if (address.plan_id, address.plan_version)
                != (generation.plan_id, generation.plan_version)
                || address.stored_output_id != reference.stored_output_id
                || self
                    .descriptors
                    .stored_output_reference(address.stored_output_id)
                    .as_ref()
                    != Some(reference)
            {
                return Err("native revision differs from installed binding".into());
            }
            let batch = batches
                .get(&(
                    address.stored_output_id,
                    address.window.start_ms,
                    address.window.end_ms,
                ))
                .ok_or("native output is absent from pinned revision")?;
            if batch.schema() != &expected_schema {
                return Err("native revision schema differs from installed contract".into());
            }
            if batch.bytes() > max_bytes {
                return Err(asap_physical_operators::Error::MemoryLimit.into());
            }
            return Ok(batch.clone());
        }
        let handles = self.storage_handles_for_output(reference);
        let [sid] = handles.as_slice() else {
            return Err("native stored output is absent or ambiguous".into());
        };
        self.read_native_summary_handle(*sid, address, reference, expected_schema, max_bytes)
    }

    fn read_native_summary_handle(
        &self,
        sid: u64,
        address: &asap_types::sds::StoredSummaryKey,
        reference: &StoredOutputReference,
        expected_schema: Schema,
        max_bytes: usize,
    ) -> Result<Batch, NativeReadError> {
        address.validate().map_err(|e| e.to_string())?;
        let generation = self
            .active_catalog_generation()
            .ok_or("native read has no active generation")?;
        if (address.plan_id, address.plan_version) != (generation.plan_id, generation.plan_version)
            || address.stored_output_id != reference.stored_output_id
            || self.stored_output_for_handle(sid).as_ref() != Some(reference)
        {
            return Err("native read differs from its installed output binding".into());
        }
        let window = (
            u64::try_from(address.window.start_ms).map_err(|_| "negative native window")?,
            u64::try_from(address.window.end_ms).map_err(|_| "negative native window")?,
        );
        let frozen = self.read_frozen_windows_with(
            sid,
            address.stored_output_id,
            &generation,
            &BTreeSet::from([window]),
            &address.population,
            |name, tag, bytes| {
                if name != NATIVE_OUTPUT_TYPE || tag != NATIVE_OUTPUT_TAG {
                    return Err("stored record is not a native physical output".into());
                }
                if bytes.len() > max_bytes {
                    return Err(NativeReadError::Physical(
                        asap_physical_operators::Error::MemoryLimit,
                    ));
                }
                let batch = decode_batch(bytes, expected_schema.clone(), max_bytes)?;
                if batch.bytes() > max_bytes {
                    return Err(NativeReadError::Physical(
                        asap_physical_operators::Error::MemoryLimit,
                    ));
                }
                let output = NativeSummaryOutput::new(batch, max_bytes)?;
                output.validate_group(&address.population)?;
                Ok(Box::new(output) as Box<dyn AggregateCore>)
            },
        )?;
        let state = frozen.windows[&window]
            .as_any()
            .downcast_ref::<NativeSummaryOutput>()
            .ok_or("native output decoder mismatch")?;
        Ok(state.batch.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::types::PrecomputedOutput;
    use asap_physical_operators::{
        dag::{operators::Operator, Limits, RunContext, Scope},
        physical_planner::{CompiledPhysicalDag, InputContract, Source},
        summary_kernels::SumAccumulator,
    };
    use futures::{executor::block_on, StreamExt};
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::DataType,
    };

    fn run(input: Batch, operator: Operator) -> Batch {
        let schema = input.schema().clone();
        let plan = CompiledPhysicalDag::from_operators(
            BTreeMap::from([(0, InputContract::bounded(schema.clone()))]),
            BTreeMap::from([(1, (vec![0], operator))]),
            vec![1],
        )
        .unwrap();
        let plan = CompiledPhysicalDag::decode(&plan.encode().unwrap()).unwrap();
        let graph = plan
            .instantiate(BTreeMap::from([(
                0,
                Box::new(Operator::source(schema, vec![input]).unwrap()) as Source<'_>,
            )]))
            .unwrap();
        let context = RunContext::new(
            Scope::Query {
                evaluation_time_ms: 60_000,
                revision: 0,
            },
            Limits::default(),
        )
        .unwrap();
        let stream = graph.execute(&[1], context).unwrap().remove(0);
        let output = block_on(stream.collect::<Vec<_>>());
        assert_eq!(output.len(), 1);
        (*output.into_iter().next().unwrap().unwrap()).clone()
    }

    // Group columns in the payload must agree with the bound record address.
    #[test]
    fn native_summary_payload_cannot_be_relabelled_as_another_group() {
        let family = SummaryFamilyType::ExactAggregate(
            planner_types::post_asap::ExactKind::Sum,
            planner_types::post_asap::ExactParams::Sum,
        );
        let schema = Arc::new(SummarySchema {
            fields: vec![
                SummaryField {
                    name: "job".into(),
                    dtype: SummaryFamilyType::Plain(DataType::Utf8),
                    nullable: false,
                },
                SummaryField {
                    name: "state".into(),
                    dtype: family.clone(),
                    nullable: false,
                },
            ],
            time_index: None,
        });
        let batch = Batch::try_new(
            schema,
            vec![vec![
                Value::Utf8("api".into()),
                Value::Summary {
                    family,
                    state: Arc::new(SumAccumulator::new()),
                },
            ]],
        )
        .unwrap();
        let output = NativeSummaryOutput::new(batch, 1 << 20).unwrap();
        output
            .validate_group(&BTreeMap::from([("job".into(), "api".into())]))
            .unwrap();
        assert!(output
            .validate_group(&BTreeMap::from([("job".into(), "worker".into())]))
            .is_err());
        assert!(output
            .validate_group(&BTreeMap::from([("unknown".into(), "api".into())]))
            .is_err());
    }

    // Real durable source panes -> native build -> immutable publication -> bound
    // lookup -> native readout, repeated after both store and resolver restart.
    #[test]
    fn native_summary_publication_and_bound_recovery_preserve_identity_and_format() {
        let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .unwrap();
        let mut query = fixture["query_workload"]["repeating_queries"][3].clone();
        query["query"] = "quantile(1.0, sum_over_time(immutable_value[1m]))".into();
        query["demand"]["fixed_interval_at"]["interval"] = 60_000.into();
        query["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
        fixture["query_workload"]["repeating_queries"] = serde_json::json!([query]);
        let snapshot = serde_json::from_value(fixture).unwrap();
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let source = plan
            .precompute_plan
            .materializations
            .iter()
            .find(|c| c.derived_input.is_none())
            .unwrap();
        let target = plan
            .precompute_plan
            .materializations
            .iter()
            .find(|c| c.derived_input.is_some())
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let resolver_path = directory.path().join("resolver.jsonl");
        let resolver = SeriesIdResolver::open(resolver_path.clone()).unwrap();
        let persistence_config = || {
            let mut config = persistence::config::SketchStorePersistenceConfig::with_memory_limit(
                1 << 24,
                directory.path().join("store"),
            );
            config.delete_older_than_ms = None;
            config.hot_window_ms = None;
            config
        };
        let store = Arc::new(SketchStore::new());
        store
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        let generation = store.active_catalog_generation().unwrap();
        let mut persistence = store.start_persistence(persistence_config()).unwrap();
        for (instance, value) in [("a", 0.125), ("b", 0.25)] {
            let group = BTreeMap::from([("instance".to_string(), instance.to_string())]);
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                stored_output_id: source.policy_fingerprint().into(),
                time_range: HalfOpenTimeRange {
                    start_ms: 0,
                    end_ms: 60_000,
                },
                group_values: group.clone(),
            };
            let revision = store
                .admit_summary_updates(&generation, BTreeSet::from([coordinate.clone()]))
                .unwrap();
            let mut output = PrecomputedOutput::new(0, 60_000, None, source.policy_fingerprint());
            output.population_labels = Some(group);
            output.catalog_generation = Some(generation.clone());
            let mut state = SumAccumulator::new();
            state.update(value);
            store
                .publish_admitted_summary_update(
                    &generation,
                    &coordinate,
                    revision,
                    revision,
                    120_000,
                    |writer| writer.ingest_precompute_with_series_id(700, source, &output, &state),
                )
                .unwrap();
        }
        let deadline = crate::tests::test_utilities::timing::deadline(Duration::from_secs(5));
        while !store.seal_finite_summary_input(&generation).unwrap() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        let cohort = store
            .read_complete_raw_maintenance_cohort(
                &generation,
                &target.derived_input.as_ref().unwrap().inputs,
                (0, 60_000),
            )
            .unwrap();
        let values = cohort
            .inputs()
            .iter()
            .flat_map(|input| input.windows.values())
            .map(|state| {
                vec![Value::Float64(
                    state
                        .query_statistic(asap_types::Statistic::Sum, &None, &HashMap::new())
                        .unwrap(),
                )]
            })
            .collect();
        let raw_schema = Arc::new(SummarySchema {
            fields: vec![SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            }],
            time_index: None,
        });
        let input = Batch::try_new(raw_schema.clone(), values).unwrap();
        let build = Operator::summary_build(
            raw_schema,
            target.accumulator_spec().unwrap().family,
            0,
            None,
            vec![],
        )
        .unwrap();
        let output_batch = run(input, build);
        let expected_schema = output_batch.schema().clone();
        let mut output = PrecomputedOutput::new(0, 60_000, None, target.policy_fingerprint());
        output.catalog_generation = Some(generation.clone());
        let before = persistence.manifest.live_parts().len();
        assert!(store
            .publish_native_summary_output(
                &resolver,
                target,
                &output,
                output_batch.clone(),
                1 << 20
            )
            .unwrap());
        store
            .publish_native_summary_output(&resolver, target, &output, output_batch, 1 << 20)
            .unwrap();
        assert_eq!(persistence.manifest.live_parts().len(), before + 1);
        let reference = plan
            .summary_catalog
            .output_reference(target.policy_fingerprint().into())
            .unwrap();
        let address = asap_types::sds::StoredSummaryKey {
            plan_id: generation.plan_id,
            plan_version: generation.plan_version,
            stored_output_id: reference.stored_output_id,
            population: BTreeMap::new(),
            window: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 60_000,
            },
        };
        let check = |store: &SketchStore, resolver: &SeriesIdResolver| {
            let batch = store
                .read_native_summary_output(
                    resolver,
                    &address,
                    &reference,
                    expected_schema.clone(),
                    1 << 20,
                )
                .unwrap();
            let direct = store
                .read_bound_native_summary(&address, &reference, expected_schema.clone(), 1 << 20)
                .unwrap();
            assert_eq!(
                encode_batch(&direct).unwrap(),
                encode_batch(&batch).unwrap()
            );
            let handles = store.storage_handles_for_output(&reference);
            assert_eq!(handles.len(), 1);
            let frames = store.query_range(handles[0], 0, 60_000);
            assert_eq!(
                frames[0].samples[&60_000][0].encoding,
                SketchEncoding::NativeBatchV1
            );
            let count = resolver.len();
            let mut other_group = address.clone();
            other_group
                .population
                .insert("instance".into(), "unknown".into());
            assert!(store
                .read_native_summary_output(
                    resolver,
                    &other_group,
                    &reference,
                    expected_schema.clone(),
                    1 << 20
                )
                .is_err());
            assert_eq!(
                resolver.len(),
                count,
                "bound reads must not allocate outputs"
            );
            let bytes = encode_batch(&batch).unwrap();
            let readout = Operator::readout(
                batch.schema().clone(),
                0,
                asap_types::Statistic::Quantile,
                HashMap::from([("quantile".into(), "1.0".into())]),
            )
            .unwrap();
            let values = run(batch, readout);
            assert!(matches!(values.rows()[0][0], Value::Float64(v) if (v - 0.25).abs() < 0.01));
            let mut wrong = reference.clone();
            wrong.definition_id = plan
                .summary_catalog
                .output_reference(source.policy_fingerprint().into())
                .unwrap()
                .definition_id;
            assert!(store
                .read_native_summary_output(
                    resolver,
                    &address,
                    &wrong,
                    expected_schema.clone(),
                    1 << 20
                )
                .is_err());
            let mut missing = address.clone();
            missing.window.end_ms = 120_000;
            assert!(store
                .read_native_summary_output(
                    resolver,
                    &missing,
                    &reference,
                    expected_schema.clone(),
                    1 << 20
                )
                .is_err());
            assert!(matches!(
                store.read_native_summary_output(
                    resolver,
                    &address,
                    &reference,
                    expected_schema.clone(),
                    1
                ),
                Err(NativeReadError::Physical(
                    asap_physical_operators::Error::MemoryLimit
                ))
            ));
            bytes
        };
        let bytes = check(&store, &resolver);
        persistence.shutdown();
        drop(store);
        drop(resolver);
        let recovered = Arc::new(SketchStore::new());
        recovered
            .install_summary_catalog(Arc::new(plan.summary_catalog.clone()))
            .unwrap();
        let mut persistence = recovered.start_persistence(persistence_config()).unwrap();
        let resolver = SeriesIdResolver::open(resolver_path).unwrap();
        assert_eq!(check(&recovered, &resolver), bytes);
        persistence.shutdown();
    }
}
