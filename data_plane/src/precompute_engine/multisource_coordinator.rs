//! Keyed, watermark-gated staging for multi-source maintenance DAG nodes.

use super::coordination_checkpoint::{
    AtomicPublicationKey, StagedSummaryInput, SummaryCoordinationCheckpointStore,
};
use asap_types::sds::{
    CatalogGeneration, HalfOpenTimeRange, SummaryInstanceCoordinates, SummaryInstanceId,
    SummarySourcePartition, SummaryStateReference, SummaryWatermarkBarrier,
};
use asap_types::PolicyFingerprint;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogicalSourcePartition {
    pub producer_id: String,
    pub partition_id: String,
}

#[derive(Debug, Clone)]
pub struct CoordinatedInput {
    pub input_node_id: String,
    pub summary_definition_id: asap_types::sds::SummaryDefinitionId,
    pub partitions: BTreeSet<LogicalSourcePartition>,
}

#[derive(Debug, Clone)]
pub struct MultiSourceNodeSpec {
    pub catalog_generation: CatalogGeneration,
    pub dag_id: String,
    pub consumer_node_id: String,
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
    checkpoint_store: SummaryCoordinationCheckpointStore,
    transition: Mutex<()>,
}

impl MultiSourceCoordinator {
    pub fn new(
        spec: MultiSourceNodeSpec,
        checkpoint_store: SummaryCoordinationCheckpointStore,
    ) -> io::Result<Self> {
        validate_spec(&spec)?;
        Ok(Self {
            spec,
            checkpoint_store,
            transition: Mutex::new(()),
        })
    }

    pub fn stage(&self, input: StagedSummaryInput) -> io::Result<bool> {
        let _transition = self
            .transition
            .lock()
            .map_err(|_| io::Error::other("multi-source coordinator lock poisoned"))?;
        self.validate_input(&input)?;
        let already_staged = self
            .checkpoint_store
            .staged()?
            .iter()
            .any(|existing| same_input_identity(existing, &input));
        if !already_staged
            && self.checkpoint_store.watermarks()?.iter().any(|barrier| {
                barrier.catalog_generation == input.catalog_generation
                    && barrier.source == input.source
                    && barrier.watermark_ms >= input.coordinates.time_range.end_ms
            })
        {
            return Err(invalid(
                "new input arrived after its source epoch completed the window",
            ));
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
        self.checkpoint_store.advance_watermark(barrier)
    }

    pub fn ready_batches(&self) -> io::Result<Vec<ReadyInputBatch>> {
        let _transition = self
            .transition
            .lock()
            .map_err(|_| io::Error::other("multi-source coordinator lock poisoned"))?;
        let staged = self.checkpoint_store.staged()?;
        let watermarks = self.checkpoint_store.watermarks()?;
        let mut buckets =
            BTreeMap::<(i64, i64, Vec<(String, String)>), Vec<StagedSummaryInput>>::new();
        for input in staged.into_iter().filter(|input| {
            input.catalog_generation == self.spec.catalog_generation
                && input.dag_id == self.spec.dag_id
                && input.consumer_node_id == self.spec.consumer_node_id
        }) {
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
            if self.complete(&inputs, &watermarks, end_ms) {
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

    fn complete(
        &self,
        inputs: &[StagedSummaryInput],
        watermarks: &[SummaryWatermarkBarrier],
        end_ms: i64,
    ) -> bool {
        self.spec.inputs.iter().all(|requirement| {
            requirement.partitions.iter().all(|partition| {
                let active_epoch = watermarks
                    .iter()
                    .filter(|barrier| {
                        barrier.catalog_generation == self.spec.catalog_generation
                            && logical(&barrier.source) == *partition
                    })
                    .map(|barrier| barrier.source.producer_epoch)
                    .max();
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

fn validate_spec(spec: &MultiSourceNodeSpec) -> io::Result<()> {
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
        coordinator.advance_watermark(barrier("0", 1, 10)).unwrap();
        coordinator.advance_watermark(barrier("1", 1, 10)).unwrap();
        assert!(coordinator.ready_batches().unwrap().is_empty());
        coordinator.advance_watermark(barrier("0", 2, 10)).unwrap();
        coordinator.advance_watermark(barrier("1", 2, 10)).unwrap();
        assert_eq!(coordinator.ready_batches().unwrap().len(), 1);
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
