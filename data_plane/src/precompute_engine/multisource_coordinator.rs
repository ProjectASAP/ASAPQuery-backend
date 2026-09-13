//! Keyed, watermark-gated staging for multi-source maintenance DAG nodes.

use super::coordination_checkpoint::{
    AtomicPublicationKey, StagedSummaryInput, SummaryCoordinationCheckpointStore,
};
use asap_types::sds::{
    CatalogGeneration, HalfOpenTimeRange, SummaryInstanceCoordinates, SummaryInstanceId,
    SummarySourcePartition, SummaryStateReference, SummaryWatermarkBarrier,
};
use asap_types::PolicyFingerprint;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalSourcePartition {
    pub producer_id: String,
    pub partition_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatedInput {
    pub input_node_id: String,
    pub summary_definition_id: asap_types::sds::SummaryDefinitionId,
    pub partitions: BTreeSet<LogicalSourcePartition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiSourceNodeSpec {
    pub catalog_generation: CatalogGeneration,
    pub dag_id: String,
    pub consumer_node_id: String,
    /// The content-addressed installed output binds its source/window contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_definition: Option<asap_types::sds::SummaryDefinitionId>,
    pub inputs: Vec<CoordinatedInput>,
    /// Named output grouping. An empty projection represents one global group.
    pub output_grouping: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyInputBatch {
    pub time_range: HalfOpenTimeRange,
    pub group_values: BTreeMap<String, String>,
    /// Inputs are ordered by the spec's input order, then source partition.
    pub inputs: Vec<StagedSummaryInput>,
}

/// Serializes stage/barrier/readiness transitions around the durable checkpoint store.
/// This is coordination, not operator execution: family-specific joins consume
/// a `ReadyInputBatch` through the typed maintenance operator registry.
pub struct MultiSourceCoordinator {
    spec: MultiSourceNodeSpec,
    installed_plan: Option<Arc<asap_types::precompute_plan::PrecomputePlan>>,
    checkpoint_store: SummaryCoordinationCheckpointStore,
    transition: Mutex<()>,
}

impl MultiSourceCoordinator {
    pub fn new(
        spec: MultiSourceNodeSpec,
        checkpoint_store: SummaryCoordinationCheckpointStore,
    ) -> io::Result<Self> {
        if spec.output_definition.is_some() {
            return Err(invalid(
                "installed coordinator requires its authoritative plan",
            ));
        }
        Self::from_scope(spec, checkpoint_store)
    }

    fn from_scope(
        spec: MultiSourceNodeSpec,
        checkpoint_store: SummaryCoordinationCheckpointStore,
    ) -> io::Result<Self> {
        validate_spec(&spec)?;
        checkpoint_store.bind_coordinator_scope(&spec)?;
        Ok(Self {
            spec,
            installed_plan: None,
            checkpoint_store,
            transition: Mutex::new(()),
        })
    }

    /// Bind the complete installed producer roster before accepting any input
    /// or barrier. This validates scope; transport authentication and durable
    /// source payload publication remain caller obligations.
    pub fn for_installed_plan(
        spec: MultiSourceNodeSpec,
        plan: Arc<asap_types::precompute_plan::PrecomputePlan>,
        checkpoint_store: SummaryCoordinationCheckpointStore,
    ) -> io::Result<Self> {
        plan.validate()
            .map_err(|error| invalid(error.to_string()))?;
        if plan.summary_catalog.as_ref() != Some(&spec.catalog_generation)
            || plan.envelope.plan_id != spec.catalog_generation.plan_id
            || plan.envelope.plan_version != spec.catalog_generation.plan_version
        {
            return Err(invalid(
                "coordinator generation differs from installed plan",
            ));
        }
        let target = spec
            .output_definition
            .ok_or_else(|| invalid("installed coordinator needs an output definition"))?;
        let config = plan
            .materializations
            .iter()
            .find(|config| config.policy_fingerprint() == target.fingerprint())
            .ok_or_else(|| invalid("coordinator output is not installed"))?;
        let expected = &config
            .derived_input
            .as_ref()
            .ok_or_else(|| invalid("coordinator output needs derived input"))?
            .inputs;
        let supplied: BTreeSet<_> = spec
            .inputs
            .iter()
            .map(|input| input.summary_definition_id)
            .collect();
        if &supplied != expected
            || supplied.len() != spec.inputs.len()
            || config.partitioning != Some(asap_types::sds::PopulationPartitioning::Grouped)
            || spec
                .output_grouping
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                != config.grouping_labels.names().into_iter().collect()
        {
            return Err(invalid(
                "coordinator source set or output grouping differs from installed target",
            ));
        }
        let mut sources = Vec::new();
        for input in &spec.inputs {
            let source = plan
                .materializations
                .iter()
                .find(|config| {
                    config.policy_fingerprint() == input.summary_definition_id.fingerprint()
                })
                .ok_or_else(|| invalid("coordinator input is not installed"))?;
            sources.push(source);
            let producers: Vec<_> = plan
                .producers
                .iter()
                .filter(|producer| producer.materialization == input.summary_definition_id)
                .collect();
            if producers.is_empty()
                || producers
                    .iter()
                    .any(|producer| producer.partition_ids.is_empty())
            {
                return Err(invalid(
                    "coordinator input has no complete authoritative partition roster",
                ));
            }
            let expected_partitions = producers
                .into_iter()
                .flat_map(|producer| {
                    producer
                        .partition_ids
                        .iter()
                        .map(move |partition| LogicalSourcePartition {
                            producer_id: producer.producer_id.clone(),
                            partition_id: partition.clone(),
                        })
                })
                .collect::<BTreeSet<_>>();
            if input.partitions != expected_partitions {
                return Err(invalid(
                    "coordinator input omits or adds installed producer partitions",
                ));
            }
        }
        asap_types::precompute_plan::validated_source_window_cohort(config, &sources)
            .map_err(|error| invalid(error.to_string()))?;
        let mut coordinator = Self::from_scope(spec, checkpoint_store)?;
        coordinator.installed_plan = Some(plan);
        for input in coordinator.checkpoint_store.staged()? {
            coordinator.validate_input(&input)?;
        }
        for barrier in coordinator.checkpoint_store.watermarks()? {
            if barrier.catalog_generation != coordinator.spec.catalog_generation
                || !coordinator
                    .logical_partitions()
                    .contains(&logical(&barrier.source))
            {
                return Err(invalid("restored barrier is outside installed scope"));
            }
            for input in &coordinator.spec.inputs {
                if input.partitions.contains(&logical(&barrier.source)) {
                    coordinator
                        .installed_plan
                        .as_ref()
                        .unwrap()
                        .validate_watermark_scope(input.summary_definition_id, &barrier)
                        .map_err(|error| invalid(error.to_string()))?;
                }
            }
        }
        Ok(coordinator)
    }

    pub fn stage(&self, input: StagedSummaryInput) -> io::Result<bool> {
        let _transition = self
            .transition
            .lock()
            .map_err(|_| io::Error::other("multi-source coordinator lock poisoned"))?;
        self.validate_input(&input)?;
        let staged = self.checkpoint_store.staged()?;
        let watermarks = self.checkpoint_store.watermarks()?;
        let already_staged = staged
            .iter()
            .any(|existing| same_input_identity(existing, &input));
        if !already_staged {
            if self
                .active_epochs(&staged, &watermarks)
                .get(&logical(&input.source))
                .is_some_and(|epoch| input.source.producer_epoch < *epoch)
            {
                return Err(invalid("new input belongs to a superseded producer epoch"));
            }
            if watermarks.iter().any(|barrier| {
                barrier.catalog_generation == input.catalog_generation
                    && barrier.source == input.source
                    && barrier.watermark_ms >= input.coordinates.time_range.end_ms
            }) {
                return Err(invalid(
                    "new input arrived after its source epoch completed the window",
                ));
            }
        }
        self.checkpoint_store.stage_if_absent(input)
    }

    pub fn advance_watermark(&self, barrier: SummaryWatermarkBarrier) -> io::Result<bool> {
        let _transition = self
            .transition
            .lock()
            .map_err(|_| io::Error::other("multi-source coordinator lock poisoned"))?;
        if barrier.catalog_generation != self.spec.catalog_generation
            || !self
                .logical_partitions()
                .contains(&logical(&barrier.source))
        {
            return Err(invalid(
                "watermark does not belong to this maintenance node",
            ));
        }
        barrier
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        if let Some(plan) = &self.installed_plan {
            for input in self
                .spec
                .inputs
                .iter()
                .filter(|input| input.partitions.contains(&logical(&barrier.source)))
            {
                plan.validate_watermark_scope(input.summary_definition_id, &barrier)
                    .map_err(|error| invalid(error.to_string()))?;
            }
        }
        let staged = self.checkpoint_store.staged()?;
        let watermarks = self.checkpoint_store.watermarks()?;
        if self
            .active_epochs(&staged, &watermarks)
            .get(&logical(&barrier.source))
            .is_some_and(|epoch| barrier.source.producer_epoch < *epoch)
        {
            return Err(invalid("watermark belongs to a superseded producer epoch"));
        }
        self.checkpoint_store.advance_watermark(barrier)
    }

    pub fn ready_batches(&self) -> io::Result<Vec<ReadyInputBatch>> {
        let _transition = self
            .transition
            .lock()
            .map_err(|_| io::Error::other("multi-source coordinator lock poisoned"))?;
        let staged = self.checkpoint_store.staged()?;
        let watermarks = self.checkpoint_store.watermarks()?;
        let active_epochs = self.active_epochs(&staged, &watermarks);
        let mut buckets =
            BTreeMap::<(i64, i64, Vec<(String, String)>), Vec<StagedSummaryInput>>::new();
        for input in staged.into_iter().filter(|input| {
            input.catalog_generation == self.spec.catalog_generation
                && input.dag_id == self.spec.dag_id
                && input.consumer_node_id == self.spec.consumer_node_id
                && active_epochs.get(&logical(&input.source)) == Some(&input.source.producer_epoch)
        }) {
            self.validate_input(&input)?;
            let projected =
                project_group(&input.coordinates.group_values, &self.spec.output_grouping)?;
            buckets
                .entry((
                    input.coordinates.time_range.start_ms,
                    input.coordinates.time_range.end_ms,
                    projected.into_iter().collect(),
                ))
                .or_default()
                .push(input);
        }

        let mut ready = Vec::new();
        for ((start_ms, end_ms, group), inputs) in buckets {
            if self.complete(&inputs, &watermarks, &active_epochs, end_ms) {
                let mut ordered = Vec::new();
                for requirement in &self.spec.inputs {
                    let mut matching = inputs
                        .iter()
                        .filter(|input| input.input_node_id == requirement.input_node_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    matching.sort_by(|a, b| a.source.cmp(&b.source));
                    ordered.extend(matching);
                }
                ready.push(ReadyInputBatch {
                    time_range: HalfOpenTimeRange { start_ms, end_ms },
                    group_values: group.into_iter().collect(),
                    inputs: ordered,
                });
            }
        }
        Ok(ready)
    }

    pub fn publication_key(
        &self,
        batch: &ReadyInputBatch,
        sink_node_id: impl Into<String>,
        instance_id: SummaryInstanceId,
        output_definition: PolicyFingerprint,
        output_lineage: Vec<u8>,
        state_reference: SummaryStateReference,
    ) -> AtomicPublicationKey {
        AtomicPublicationKey {
            catalog_generation: self.spec.catalog_generation.clone(),
            dag_id: self.spec.dag_id.clone(),
            sink_node_id: sink_node_id.into(),
            instance_id,
            coordinates: SummaryInstanceCoordinates {
                summary_definition_id: output_definition.into(),
                time_range: batch.time_range,
                group_values: batch.group_values.clone(),
            },
            output_lineage,
            state_reference,
        }
    }

    fn validate_input(&self, input: &StagedSummaryInput) -> io::Result<()> {
        if input.catalog_generation != self.spec.catalog_generation
            || input.dag_id != self.spec.dag_id
            || input.consumer_node_id != self.spec.consumer_node_id
        {
            return Err(invalid("staged input belongs to another plan or node"));
        }
        let requirement = self
            .spec
            .inputs
            .iter()
            .find(|requirement| requirement.input_node_id == input.input_node_id)
            .ok_or_else(|| invalid("staged input node is not required"))?;
        if input.coordinates.summary_definition_id != requirement.summary_definition_id
            || !requirement.partitions.contains(&logical(&input.source))
        {
            return Err(invalid(
                "staged input source does not match its requirement",
            ));
        }
        if let Some(plan) = &self.installed_plan {
            let config = plan
                .materializations
                .iter()
                .find(|config| {
                    config.policy_fingerprint() == requirement.summary_definition_id.fingerprint()
                })
                .ok_or_else(|| invalid("staged input definition is not installed"))?;
            let window = input.coordinates.time_range;
            let width = i64::try_from(config.stored_window_ms())
                .map_err(|_| invalid("source window width overflow"))?;
            let slide = config
                .slide_interval
                .checked_mul(1000)
                .filter(|slide| *slide > 0)
                .ok_or_else(|| invalid("source window slide is invalid"))?;
            if window.end_ms.checked_sub(window.start_ms) != Some(width)
                || (i128::from(window.start_ms) - i128::from(config.pane_origin_ms.unwrap_or(0)))
                    .rem_euclid(i128::from(slide))
                    != 0
            {
                return Err(invalid(
                    "staged input window differs from installed source contract",
                ));
            }
        }
        project_group(&input.coordinates.group_values, &self.spec.output_grouping)?;
        Ok(())
    }

    fn logical_partitions(&self) -> BTreeSet<LogicalSourcePartition> {
        self.spec
            .inputs
            .iter()
            .flat_map(|input| input.partitions.iter().cloned())
            .collect()
    }

    /// A staged input already observes a new epoch; waiting until its first
    /// watermark would allow the previous epoch's barrier to authorize work.
    /// Historical staged metadata remains available for audit/idempotent retry.
    fn active_epochs(
        &self,
        staged: &[StagedSummaryInput],
        watermarks: &[SummaryWatermarkBarrier],
    ) -> BTreeMap<LogicalSourcePartition, u64> {
        let partitions = self.logical_partitions();
        let mut active = BTreeMap::<LogicalSourcePartition, u64>::new();
        let sources = staged
            .iter()
            .filter(|input| input.catalog_generation == self.spec.catalog_generation)
            .map(|input| &input.source)
            .chain(
                watermarks
                    .iter()
                    .filter(|barrier| barrier.catalog_generation == self.spec.catalog_generation)
                    .map(|barrier| &barrier.source),
            );
        for source in sources {
            let partition = logical(source);
            if partitions.contains(&partition) {
                let epoch = active.entry(partition).or_default();
                *epoch = (*epoch).max(source.producer_epoch);
            }
        }
        active
    }

    fn complete(
        &self,
        inputs: &[StagedSummaryInput],
        watermarks: &[SummaryWatermarkBarrier],
        active_epochs: &BTreeMap<LogicalSourcePartition, u64>,
        end_ms: i64,
    ) -> bool {
        self.spec.inputs.iter().all(|requirement| {
            requirement.partitions.iter().all(|partition| {
                let active_epoch = active_epochs.get(partition).copied();
                active_epoch.is_some_and(|epoch| {
                    let source = SummarySourcePartition {
                        producer_id: partition.producer_id.clone(),
                        partition_id: partition.partition_id.clone(),
                        producer_epoch: epoch,
                    };
                    watermarks.iter().any(|barrier| {
                        barrier.catalog_generation == self.spec.catalog_generation
                            && barrier.source == source
                            && barrier.watermark_ms >= end_ms
                    }) && inputs.iter().any(|input| {
                        input.input_node_id == requirement.input_node_id && input.source == source
                    })
                })
            })
        })
    }
}

pub(crate) fn validate_spec(spec: &MultiSourceNodeSpec) -> io::Result<()> {
    if spec.dag_id.trim().is_empty()
        || spec.consumer_node_id.trim().is_empty()
        || spec.inputs.len() < 2
    {
        return Err(invalid(
            "multi-source spec needs IDs and at least two inputs",
        ));
    }
    let mut nodes = BTreeSet::new();
    for input in &spec.inputs {
        if input.input_node_id.trim().is_empty()
            || input.partitions.is_empty()
            || !nodes.insert(input.input_node_id.as_str())
            || input
                .partitions
                .iter()
                .any(|p| p.producer_id.trim().is_empty() || p.partition_id.trim().is_empty())
        {
            return Err(invalid("multi-source input requirement is invalid"));
        }
    }
    let mut grouping = BTreeSet::new();
    if spec
        .output_grouping
        .iter()
        .any(|key| key.trim().is_empty() || !grouping.insert(key))
    {
        return Err(invalid(
            "output grouping contains an empty or duplicate key",
        ));
    }
    Ok(())
}

fn project_group(
    values: &BTreeMap<String, String>,
    keys: &[String],
) -> io::Result<BTreeMap<String, String>> {
    keys.iter()
        .map(|key| {
            values
                .get(key)
                .cloned()
                .map(|value| (key.clone(), value))
                .ok_or_else(|| invalid(format!("input is missing grouping label {key}")))
        })
        .collect()
}

fn logical(source: &SummarySourcePartition) -> LogicalSourcePartition {
    LogicalSourcePartition {
        producer_id: source.producer_id.clone(),
        partition_id: source.partition_id.clone(),
    }
}

fn same_input_identity(a: &StagedSummaryInput, b: &StagedSummaryInput) -> bool {
    a.instance_id == b.instance_id
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn generation() -> CatalogGeneration {
        CatalogGeneration {
            schema_version: 1,
            plan_id: 1,
            plan_version: 1,
            snapshot_sha256: "sha".into(),
        }
    }
    fn partition(id: &str) -> LogicalSourcePartition {
        LogicalSourcePartition {
            producer_id: "p".into(),
            partition_id: id.into(),
        }
    }
    fn spec() -> MultiSourceNodeSpec {
        MultiSourceNodeSpec {
            catalog_generation: generation(),
            dag_id: "dag".into(),
            consumer_node_id: "join".into(),
            output_definition: None,
            inputs: vec![
                CoordinatedInput {
                    input_node_id: "left".into(),
                    summary_definition_id: PolicyFingerprint(1).into(),
                    partitions: BTreeSet::from([partition("0")]),
                },
                CoordinatedInput {
                    input_node_id: "right".into(),
                    summary_definition_id: PolicyFingerprint(2).into(),
                    partitions: BTreeSet::from([partition("1")]),
                },
            ],
            output_grouping: vec!["job".into()],
        }
    }
    fn input(node: &str, definition: u64, partition_id: &str, epoch: u64) -> StagedSummaryInput {
        StagedSummaryInput {
            catalog_generation: generation(),
            dag_id: "dag".into(),
            consumer_node_id: "join".into(),
            input_node_id: node.into(),
            source: SummarySourcePartition {
                producer_id: "p".into(),
                partition_id: partition_id.into(),
                producer_epoch: epoch,
            },
            instance_id: SummaryInstanceId::new(format!("{node}-{epoch}")).unwrap(),
            coordinates: SummaryInstanceCoordinates {
                summary_definition_id: PolicyFingerprint(definition).into(),
                time_range: HalfOpenTimeRange {
                    start_ms: 0,
                    end_ms: 10,
                },
                group_values: BTreeMap::from([
                    ("job".into(), "api".into()),
                    ("instance".into(), node.into()),
                ]),
            },
            input_lineage: vec![definition as u8, epoch as u8],
            state_reference: SummaryStateReference {
                store: "summary-store".into(),
                key: format!("{node}/{epoch}"),
                state_schema_version: 1,
                generation: 1,
                sequence: 1,
                checksum: Some(format!("sha256:{definition}")),
            },
        }
    }
    fn barrier(partition_id: &str, epoch: u64, watermark_ms: i64) -> SummaryWatermarkBarrier {
        SummaryWatermarkBarrier {
            catalog_generation: generation(),
            source: SummarySourcePartition {
                producer_id: "p".into(),
                partition_id: partition_id.into(),
                producer_epoch: epoch,
            },
            sequence: 1,
            watermark_ms,
        }
    }

    fn installed_fixture() -> (
        Arc<asap_types::precompute_plan::PrecomputePlan>,
        MultiSourceNodeSpec,
    ) {
        let mut wire: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .unwrap();
        let mut query = wire["query_workload"]["repeating_queries"][3].clone();
        query["query"] = "quantile(0.9, sum_over_time(m[1m]) + sum_over_time(n[1m]))".into();
        query["demand"]["fixed_interval_at"]["interval"] = 60000.into();
        query["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
        query["time_selection"]["lookback"] = 60000.into();
        wire["query_workload"]["repeating_queries"] = serde_json::json!([query]);
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_value(wire).unwrap();
        let mut plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap()
            .precompute_plan;
        let target = plan
            .materializations
            .iter()
            .find(|config| config.derived_input.is_some())
            .unwrap();
        let mut spec = MultiSourceNodeSpec {
            catalog_generation: plan.summary_catalog.clone().unwrap(),
            dag_id: "selected-maintenance".into(),
            consumer_node_id: "global".into(),
            output_definition: Some(target.policy_fingerprint().into()),
            inputs: Vec::new(),
            output_grouping: Vec::new(),
        };
        let definitions = target.derived_input.as_ref().unwrap().inputs.clone();
        plan.producers.clear();
        for (ordinal, definition) in definitions.into_iter().enumerate() {
            let producer_id = format!("producer-{ordinal}");
            let partition_ids = BTreeSet::from(["east".into(), "west".into()]);
            plan.producers
                .push(asap_types::precompute_plan::ProducerContract {
                    producer_id: producer_id.clone(),
                    collector_id: producer_id.clone(),
                    materialization: definition,
                    schema_id: plan
                        .schemas
                        .iter()
                        .find(|schema| schema.materialization == definition)
                        .unwrap()
                        .schema_id
                        .clone(),
                    partition_ids: partition_ids.clone(),
                });
            spec.inputs.push(CoordinatedInput {
                input_node_id: format!("input-{ordinal}"),
                summary_definition_id: definition,
                partitions: partition_ids
                    .into_iter()
                    .map(|partition_id| LogicalSourcePartition {
                        producer_id: producer_id.clone(),
                        partition_id,
                    })
                    .collect(),
            });
        }
        plan.validate().unwrap();
        (Arc::new(plan), spec)
    }

    fn roster_inputs(
        spec: &MultiSourceNodeSpec,
        epoch: u64,
        start: i64,
    ) -> Vec<StagedSummaryInput> {
        spec.inputs
            .iter()
            .flat_map(|requirement| {
                requirement.partitions.iter().map(move |partition| {
                    let key = format!(
                        "{}-{}-{epoch}-{start}",
                        partition.producer_id, partition.partition_id
                    );
                    StagedSummaryInput {
                        catalog_generation: spec.catalog_generation.clone(),
                        dag_id: spec.dag_id.clone(),
                        consumer_node_id: spec.consumer_node_id.clone(),
                        input_node_id: requirement.input_node_id.clone(),
                        source: SummarySourcePartition {
                            producer_id: partition.producer_id.clone(),
                            partition_id: partition.partition_id.clone(),
                            producer_epoch: epoch,
                        },
                        instance_id: SummaryInstanceId::new(key.clone()).unwrap(),
                        coordinates: SummaryInstanceCoordinates {
                            summary_definition_id: requirement.summary_definition_id,
                            time_range: HalfOpenTimeRange {
                                start_ms: start,
                                end_ms: start + 60000,
                            },
                            group_values: BTreeMap::new(),
                        },
                        input_lineage: key.as_bytes().to_vec(),
                        state_reference: SummaryStateReference {
                            store: "durable-summary-store".into(),
                            key: key.clone(),
                            state_schema_version: 1,
                            generation: 1,
                            sequence: 1,
                            checksum: Some(format!("sha256:{key}")),
                        },
                    }
                })
            })
            .collect()
    }

    fn input_barrier(
        input: &StagedSummaryInput,
        sequence: u64,
        watermark_ms: i64,
    ) -> SummaryWatermarkBarrier {
        SummaryWatermarkBarrier {
            catalog_generation: input.catalog_generation.clone(),
            source: input.source.clone(),
            sequence,
            watermark_ms,
        }
    }

    #[test]
    fn installed_roster_barriers_require_every_producer_partition_and_survive_restart() {
        let (plan, spec) = installed_fixture();
        let temp = tempdir().unwrap();
        let path = temp.path().join("coordinator.json");
        let coordinator = MultiSourceCoordinator::for_installed_plan(
            spec.clone(),
            Arc::clone(&plan),
            SummaryCoordinationCheckpointStore::open(&path).unwrap(),
        )
        .unwrap();
        let inputs = roster_inputs(&spec, 1, 0);
        assert_eq!(inputs.len(), 4);
        for input in &inputs {
            coordinator.stage(input.clone()).unwrap();
        }
        for input in &inputs[..3] {
            coordinator
                .advance_watermark(input_barrier(input, 1, 60000))
                .unwrap();
        }
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator
            .advance_watermark(input_barrier(&inputs[3], 1, 60000))
            .unwrap();
        assert_eq!(coordinator.ready_batches().unwrap()[0].inputs.len(), 4);
        let before = std::fs::read(&path).unwrap();
        assert!(!coordinator.stage(inputs[0].clone()).unwrap());
        assert!(!coordinator
            .advance_watermark(input_barrier(&inputs[0], 1, 60000))
            .unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        for case in 0..7 {
            let mut wrong = input_barrier(&inputs[0], 1, 60000);
            match case {
                0 => wrong.source.producer_id = "foreign".into(),
                1 => wrong.source.partition_id = "missing".into(),
                2 => wrong.catalog_generation.snapshot_sha256 = "other".into(),
                3 => wrong.source.producer_epoch = 0,
                4 => wrong.sequence = 0,
                5 => wrong.watermark_ms += 1,
                _ => wrong.watermark_ms -= 1,
            }
            assert!(coordinator.advance_watermark(wrong).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
        let mut changed = inputs[0].clone();
        changed.state_reference.checksum = Some("changed".into());
        assert!(coordinator.stage(changed).is_err());
        let mut wrong_window = roster_inputs(&spec, 1, 1)[0].clone();
        wrong_window.coordinates.time_range.end_ms = 60001;
        assert!(coordinator.stage(wrong_window).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        coordinator
            .advance_watermark(input_barrier(&inputs[0], 2, 120000))
            .unwrap();
        let progress = std::fs::read(&path).unwrap();
        assert!(coordinator
            .advance_watermark(input_barrier(&inputs[0], 1, 60000))
            .is_err());
        assert!(coordinator
            .advance_watermark(input_barrier(&inputs[0], 3, 60000))
            .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), progress);
        let newer = roster_inputs(&spec, 2, 60000)[0].clone();
        coordinator.stage(newer.clone()).unwrap();
        assert!(coordinator.ready_batches().unwrap().is_empty());
        let epoch_progress = std::fs::read(&path).unwrap();
        assert!(coordinator
            .advance_watermark(input_barrier(&inputs[0], 99, 999999))
            .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), epoch_progress);
        drop(coordinator);
        let reopened = MultiSourceCoordinator::for_installed_plan(
            spec.clone(),
            Arc::clone(&plan),
            SummaryCoordinationCheckpointStore::open(&path).unwrap(),
        )
        .unwrap();
        assert!(reopened.ready_batches().unwrap().is_empty());
        assert!(reopened
            .advance_watermark(input_barrier(&inputs[0], 100, 999999))
            .is_err());
        assert!(!reopened.stage(newer).unwrap());
        drop(reopened);
        let mut reduced_plan = (*plan).clone();
        reduced_plan.producers[0].partition_ids.remove("west");
        let mut reduced_spec = spec.clone();
        reduced_spec.inputs[0]
            .partitions
            .retain(|partition| partition.partition_id != "west");
        assert!(MultiSourceCoordinator::for_installed_plan(
            reduced_spec,
            Arc::new(reduced_plan),
            SummaryCoordinationCheckpointStore::open(&path).unwrap()
        )
        .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), epoch_progress);
    }

    #[test]
    fn installed_checkpoint_cannot_downgrade_or_restore_foreign_windows() {
        let (plan, spec) = installed_fixture();
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let coordinator = MultiSourceCoordinator::for_installed_plan(
            spec.clone(),
            plan.clone(),
            SummaryCoordinationCheckpointStore::open(&path).unwrap(),
        )
        .unwrap();
        drop(coordinator);
        let before = std::fs::read(&path).unwrap();
        assert!(MultiSourceCoordinator::new(
            spec.clone(),
            SummaryCoordinationCheckpointStore::open(&path).unwrap()
        )
        .is_err());
        assert_eq!(before, std::fs::read(&path).unwrap());
        let store = SummaryCoordinationCheckpointStore::open(&path).unwrap();
        let mut malformed = roster_inputs(&spec, 1, 0).remove(0);
        malformed.coordinates.time_range.end_ms -= 1;
        store.stage_if_absent(malformed).unwrap();
        drop(store);
        let before = std::fs::read(&path).unwrap();
        assert!(MultiSourceCoordinator::for_installed_plan(
            spec,
            plan,
            SummaryCoordinationCheckpointStore::open(&path).unwrap()
        )
        .is_err());
        assert_eq!(before, std::fs::read(&path).unwrap());
    }

    #[test]
    fn installed_roster_rejects_omissions_and_nonempty_unscoped_checkpoints() {
        let (plan, spec) = installed_fixture();
        let temp = tempdir().unwrap();
        let path = temp.path().join("coordinator.json");
        let mut incomplete = spec.clone();
        incomplete.inputs[0].partitions.pop_last();
        assert!(MultiSourceCoordinator::for_installed_plan(
            incomplete,
            Arc::clone(&plan),
            SummaryCoordinationCheckpointStore::open(&path).unwrap()
        )
        .is_err());
        assert!(!path.exists());
        let input = roster_inputs(&spec, 1, 0)[0].clone();
        let legacy = SummaryCoordinationCheckpointStore::open(&path).unwrap();
        legacy
            .advance_watermark(input_barrier(&input, 1, 60000))
            .unwrap();
        drop(legacy);
        let bytes = std::fs::read(&path).unwrap();
        assert!(MultiSourceCoordinator::for_installed_plan(
            spec,
            plan,
            SummaryCoordinationCheckpointStore::open(&path).unwrap()
        )
        .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn failed_checkpoint_persistence_stops_barrier_and_readiness_until_reopen() {
        let (plan, spec) = installed_fixture();
        let temp = tempdir().unwrap();
        let path = temp.path().join("coordinator.json");
        let coordinator = MultiSourceCoordinator::for_installed_plan(
            spec.clone(),
            Arc::clone(&plan),
            SummaryCoordinationCheckpointStore::open(&path).unwrap(),
        )
        .unwrap();
        let input = roster_inputs(&spec, 1, 0)[0].clone();
        let before = std::fs::read(&path).unwrap();
        std::fs::create_dir(path.with_extension("tmp")).unwrap();
        assert!(coordinator
            .advance_watermark(input_barrier(&input, 1, 60000))
            .is_err());
        assert!(coordinator.ready_batches().is_err());
        assert!(coordinator.stage(input.clone()).is_err());
        std::fs::remove_dir(path.with_extension("tmp")).unwrap();
        assert!(coordinator
            .advance_watermark(input_barrier(&input, 1, 60000))
            .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(coordinator);
        let reopened = MultiSourceCoordinator::for_installed_plan(
            spec,
            plan,
            SummaryCoordinationCheckpointStore::open(&path).unwrap(),
        )
        .unwrap();
        assert!(reopened.checkpoint_store.watermarks().unwrap().is_empty());
        assert!(reopened
            .advance_watermark(input_barrier(&input, 1, 60000))
            .unwrap());
    }

    #[test]
    fn waits_for_every_input_and_watermark_then_projects_group() {
        let dir = tempdir().unwrap();
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(dir.path().join("checkpoint.json")).unwrap(),
        )
        .unwrap();
        coordinator.stage(input("left", 1, "0", 1)).unwrap();
        coordinator.advance_watermark(barrier("0", 1, 10)).unwrap();
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator.stage(input("right", 2, "1", 1)).unwrap();
        coordinator.advance_watermark(barrier("1", 1, 9)).unwrap();
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator
            .advance_watermark(SummaryWatermarkBarrier {
                sequence: 2,
                ..barrier("1", 1, 10)
            })
            .unwrap();
        let ready = coordinator.ready_batches().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(
            ready[0].group_values,
            BTreeMap::from([("job".into(), "api".into())])
        );
        assert_eq!(
            ready[0]
                .inputs
                .iter()
                .map(|i| i.input_node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["left", "right"]
        );
    }

    #[test]
    fn restart_preserves_readiness_and_publication_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(&path).unwrap(),
        )
        .unwrap();
        coordinator.stage(input("left", 1, "0", 1)).unwrap();
        coordinator.stage(input("right", 2, "1", 1)).unwrap();
        coordinator.advance_watermark(barrier("0", 1, 10)).unwrap();
        coordinator.advance_watermark(barrier("1", 1, 10)).unwrap();
        drop(coordinator);
        let recovered = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(path).unwrap(),
        )
        .unwrap();
        assert_eq!(recovered.ready_batches().unwrap().len(), 1);
    }

    #[test]
    fn late_new_input_is_rejected_but_exact_retry_is_idempotent() {
        let dir = tempdir().unwrap();
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(dir.path().join("checkpoint.json")).unwrap(),
        )
        .unwrap();
        let left = input("left", 1, "0", 1);
        coordinator.stage(left.clone()).unwrap();
        coordinator.advance_watermark(barrier("0", 1, 10)).unwrap();
        assert!(!coordinator.stage(left).unwrap());
        let mut late = input("left", 1, "0", 1);
        late.instance_id = SummaryInstanceId::new("late").unwrap();
        late.input_lineage = vec![9];
        assert!(coordinator.stage(late).is_err());
    }

    #[test]
    fn old_epoch_cannot_complete_new_epoch_input() {
        let dir = tempdir().unwrap();
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(dir.path().join("checkpoint.json")).unwrap(),
        )
        .unwrap();
        coordinator.stage(input("left", 1, "0", 2)).unwrap();
        coordinator.stage(input("right", 2, "1", 2)).unwrap();
        assert!(coordinator.advance_watermark(barrier("0", 1, 10)).is_err());
        assert!(coordinator.advance_watermark(barrier("1", 1, 10)).is_err());
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator.advance_watermark(barrier("0", 2, 10)).unwrap();
        coordinator.advance_watermark(barrier("1", 2, 10)).unwrap();
        assert_eq!(coordinator.ready_batches().unwrap().len(), 1);
    }

    #[test]
    fn restarted_partitions_wait_for_new_barriers_and_exclude_old_epoch_inputs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(path.clone()).unwrap(),
        )
        .unwrap();
        for (node, definition, partition) in [("left", 1, "0"), ("right", 2, "1")] {
            coordinator
                .stage(input(node, definition, partition, 1))
                .unwrap();
            coordinator
                .advance_watermark(barrier(partition, 1, 10))
                .unwrap();
        }
        assert_eq!(coordinator.ready_batches().unwrap()[0].inputs.len(), 2);
        for (node, definition, partition) in [("left", 1, "0"), ("right", 2, "1")] {
            coordinator
                .stage(input(node, definition, partition, 2))
                .unwrap();
        }
        // Observing a restarted partition invalidates its old completion proof
        // even before the new epoch has emitted its first barrier.
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator.advance_watermark(barrier("0", 2, 10)).unwrap();
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator.advance_watermark(barrier("1", 2, 10)).unwrap();
        let ready = coordinator.ready_batches().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].inputs.len(), 2);
        assert!(ready[0]
            .inputs
            .iter()
            .all(|input| input.source.producer_epoch == 2));
        drop(coordinator);
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(path).unwrap(),
        )
        .unwrap();
        assert_eq!(coordinator.ready_batches().unwrap(), ready);
        let before = coordinator.checkpoint_store.staged().unwrap();
        // Retain old metadata for replay/audit; do not turn an identical retry
        // into a new contribution or erase uncommitted historical inputs.
        assert_eq!(before.len(), 4);
        assert!(!coordinator.stage(input("left", 1, "0", 1)).unwrap());
        let mut late = input("left", 1, "0", 1);
        late.instance_id = SummaryInstanceId::new("late-old-epoch").unwrap();
        late.coordinates.time_range = HalfOpenTimeRange {
            start_ms: 20,
            end_ms: 30,
        };
        assert!(coordinator.stage(late).is_err());
        assert_eq!(coordinator.checkpoint_store.staged().unwrap(), before);
        assert_eq!(coordinator.ready_batches().unwrap(), ready);
    }

    #[test]
    fn differing_projected_groups_do_not_join() {
        let dir = tempdir().unwrap();
        let coordinator = MultiSourceCoordinator::new(
            spec(),
            SummaryCoordinationCheckpointStore::open(dir.path().join("checkpoint.json")).unwrap(),
        )
        .unwrap();
        coordinator.stage(input("left", 1, "0", 1)).unwrap();
        let mut right = input("right", 2, "1", 1);
        right
            .coordinates
            .group_values
            .insert("job".into(), "worker".into());
        coordinator.stage(right).unwrap();
        coordinator.advance_watermark(barrier("0", 1, 10)).unwrap();
        coordinator.advance_watermark(barrier("1", 1, 10)).unwrap();
        assert!(coordinator.ready_batches().unwrap().is_empty());
    }
}
