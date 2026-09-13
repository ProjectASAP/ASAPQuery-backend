//! Durable staging and publication decisions for cross-source maintenance.

use asap_types::sds::{
    CatalogGeneration, SummaryInstanceCoordinates, SummaryInstanceId, SummarySourcePartition,
    SummaryStateReference, SummaryWatermarkBarrier,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedSummaryInput {
    pub catalog_generation: CatalogGeneration,
    pub dag_id: String,
    pub consumer_node_id: String,
    pub input_node_id: String,
    pub source: SummarySourcePartition,
    pub instance_id: SummaryInstanceId,
    pub coordinates: SummaryInstanceCoordinates,
    pub input_lineage: Vec<u8>,
    /// Durable SummaryStore payload; the checkpoint store never duplicates state bytes.
    pub state_reference: SummaryStateReference,
}

impl StagedSummaryInput {
    fn validate(&self) -> io::Result<()> {
        if self.dag_id.trim().is_empty()
            || self.consumer_node_id.trim().is_empty()
            || self.input_node_id.trim().is_empty()
        {
            return Err(invalid("staged input contains an empty required field"));
        }
        let completion = asap_types::sds::SummaryWindowCompletion {
            catalog_generation: self.catalog_generation.clone(),
            source: self.source.clone(),
            instance_id: self.instance_id.clone(),
            coordinates: self.coordinates.clone(),
            input_lineage: self.input_lineage.clone(),
        };
        completion
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        validate_state_reference(&self.state_reference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtomicPublicationKey {
    pub catalog_generation: CatalogGeneration,
    pub dag_id: String,
    pub sink_node_id: String,
    pub instance_id: SummaryInstanceId,
    pub coordinates: SummaryInstanceCoordinates,
    pub output_lineage: Vec<u8>,
    pub state_reference: SummaryStateReference,
}

impl AtomicPublicationKey {
    fn validate(&self) -> io::Result<()> {
        if self.dag_id.trim().is_empty()
            || self.sink_node_id.trim().is_empty()
            || self.output_lineage.is_empty()
        {
            return Err(invalid("publication key contains an empty required field"));
        }
        self.instance_id
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        self.coordinates
            .time_range
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        validate_state_reference(&self.state_reference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointDocument {
    schema_version: u32,
    revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    coordinator_scope: Option<super::multisource_coordinator::MultiSourceNodeSpec>,
    staged: Vec<StagedSummaryInput>,
    watermarks: Vec<SummaryWatermarkBarrier>,
    published: Vec<AtomicPublicationKey>,
}

impl Default for CheckpointDocument {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            revision: 0,
            coordinator_scope: None,
            staged: Vec::new(),
            watermarks: Vec::new(),
            published: Vec::new(),
        }
    }
}

/// A small crash-safe checkpoint snapshot store. Mutations become visible in memory
/// only after the replacement file has been flushed and atomically renamed.
pub struct SummaryCoordinationCheckpointStore {
    path: PathBuf,
    /// Held for the checkpoint store lifetime; prevents two processes from replacing
    /// the same snapshot concurrently.
    _lock_file: File,
    document: Mutex<CheckpointDocument>,
    persistence_uncertain: AtomicBool,
}

impl SummaryCoordinationCheckpointStore {
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let lock_path = path.with_extension("lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock_file.try_lock_exclusive().map_err(|error| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("coordination checkpoint store already has a writer: {error}"),
            )
        })?;
        let document = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<CheckpointDocument>(&bytes).map_err(|error| {
                invalid(format!("invalid coordination checkpoint store: {error}"))
            })?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => CheckpointDocument::default(),
            Err(error) => return Err(error),
        };
        validate_document(&document)?;
        Ok(Self {
            path,
            _lock_file: lock_file,
            document: Mutex::new(document),
            persistence_uncertain: AtomicBool::new(false),
        })
    }

    pub(crate) fn bind_coordinator_scope(
        &self,
        spec: &super::multisource_coordinator::MultiSourceNodeSpec,
    ) -> io::Result<bool> {
        super::multisource_coordinator::validate_spec(spec)?;
        self.mutate(|document| {
            if let Some(existing) = &document.coordinator_scope {
                return if existing == spec {
                    Ok(false)
                } else {
                    Err(invalid("persisted coordinator scope changed"))
                };
            }
            if !document.staged.is_empty()
                || !document.watermarks.is_empty()
                || !document.published.is_empty()
            {
                return Err(invalid(
                    "nonempty legacy checkpoint has no authoritative coordinator scope",
                ));
            }
            document.coordinator_scope = Some(spec.clone());
            Ok(true)
        })
    }

    pub fn stage_if_absent(&self, input: StagedSummaryInput) -> io::Result<bool> {
        input.validate()?;
        self.mutate(|document| {
            if let Some(existing) = document
                .staged
                .iter()
                .find(|item| item.instance_id == input.instance_id)
            {
                return if existing == &input {
                    Ok(false)
                } else {
                    Err(invalid(
                        "summary instance ID was reused with different metadata",
                    ))
                };
            }
            document.staged.push(input);
            Ok(true)
        })
    }

    pub fn advance_watermark(&self, barrier: SummaryWatermarkBarrier) -> io::Result<bool> {
        barrier
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        self.mutate(|document| {
            if let Some(existing) = document.watermarks.iter_mut().find(|item| {
                item.catalog_generation == barrier.catalog_generation
                    && item.source == barrier.source
            }) {
                if barrier.sequence == existing.sequence && *existing != barrier {
                    return Err(invalid("equal watermark sequence changed its claim"));
                }
                if barrier.sequence < existing.sequence
                    || barrier.watermark_ms < existing.watermark_ms
                {
                    return Err(invalid(
                        "watermark or sequence regressed within a producer epoch",
                    ));
                }
                if *existing == barrier {
                    return Ok(false);
                }
                *existing = barrier;
            } else {
                document.watermarks.push(barrier);
            }
            Ok(true)
        })
    }

    #[cfg(test)]
    pub fn publish_if_absent(&self, key: AtomicPublicationKey) -> io::Result<bool> {
        key.validate()?;
        self.mutate(|document| {
            if let Some(existing) = document
                .published
                .iter()
                .find(|item| item.instance_id == key.instance_id)
            {
                return if existing == &key {
                    Ok(false)
                } else {
                    Err(invalid(
                        "published summary instance ID was reused with different metadata",
                    ))
                };
            }
            document.published.push(key);
            Ok(true)
        })
    }

    pub fn staged(&self) -> io::Result<Vec<StagedSummaryInput>> {
        Ok(self.lock()?.staged.clone())
    }

    pub fn watermarks(&self) -> io::Result<Vec<SummaryWatermarkBarrier>> {
        Ok(self.lock()?.watermarks.clone())
    }

    pub fn is_published(&self, key: &AtomicPublicationKey) -> io::Result<bool> {
        Ok(self.lock()?.published.contains(key))
    }

    fn mutate<T>(
        &self,
        update: impl FnOnce(&mut CheckpointDocument) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut guard = self.lock()?;
        let mut next = guard.clone();
        let result = update(&mut next)?;
        if next == *guard {
            return Ok(result);
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("coordination checkpoint store revision overflow"))?;
        if let Err(error) = persist_atomically(&self.path, &next) {
            // Rename may have succeeded before directory fsync failed. Do not
            // overwrite that possible newer checkpoint from stale live state.
            self.persistence_uncertain.store(true, Ordering::Release);
            return Err(error);
        }
        *guard = next;
        Ok(result)
    }

    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, CheckpointDocument>> {
        let guard = self
            .document
            .lock()
            .map_err(|_| io::Error::other("coordination checkpoint store lock poisoned"))?;
        if self.persistence_uncertain.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "checkpoint persistence is uncertain; reopen before continuing",
            ));
        }
        Ok(guard)
    }
}

fn validate_document(document: &CheckpointDocument) -> io::Result<()> {
    if document.schema_version != SCHEMA_VERSION {
        return Err(invalid("unsupported coordination checkpoint store schema"));
    }
    if let Some(scope) = &document.coordinator_scope {
        super::multisource_coordinator::validate_spec(scope)?;
    }
    let mut staged_ids = std::collections::BTreeSet::new();
    for input in &document.staged {
        input.validate()?;
        if !staged_ids.insert(input.instance_id.canonical()) {
            return Err(invalid("duplicate staged summary instance ID"));
        }
    }
    let mut watermark_ids = std::collections::BTreeSet::new();
    for barrier in &document.watermarks {
        barrier
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        let identity = (
            barrier.catalog_generation.schema_version,
            barrier.catalog_generation.plan_id,
            barrier.catalog_generation.plan_version,
            &barrier.catalog_generation.snapshot_sha256,
            &barrier.source,
        );
        if !watermark_ids.insert(identity) {
            return Err(invalid("duplicate source-epoch watermark"));
        }
    }
    let mut published_ids = std::collections::BTreeSet::new();
    for key in &document.published {
        key.validate()?;
        if !published_ids.insert(key.instance_id.canonical()) {
            return Err(invalid("duplicate published summary instance ID"));
        }
    }
    Ok(())
}

fn validate_state_reference(reference: &SummaryStateReference) -> io::Result<()> {
    if reference.store.trim().is_empty()
        || reference.key.trim().is_empty()
        || reference.state_schema_version == 0
        || reference
            .checksum
            .as_deref()
            .is_none_or(|checksum| checksum.trim().is_empty())
    {
        Err(invalid(
            "coordination state reference must be durable and checksummed",
        ))
    } else {
        Ok(())
    }
}

fn persist_atomically(path: &Path, document: &CheckpointDocument) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp = path.with_extension("tmp");
    let bytes = serde_json::to_vec(document).map_err(io::Error::other)?;
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{sds::HalfOpenTimeRange, sds::SummaryDefinitionId, PolicyFingerprint};
    use std::collections::BTreeMap;

    fn generation() -> CatalogGeneration {
        CatalogGeneration {
            schema_version: 1,
            plan_id: 1,
            plan_version: 2,
            snapshot_sha256: "sha".into(),
        }
    }

    fn source(epoch: u64) -> SummarySourcePartition {
        SummarySourcePartition {
            producer_id: "producer".into(),
            partition_id: "0".into(),
            producer_epoch: epoch,
        }
    }

    fn coordinates() -> SummaryInstanceCoordinates {
        SummaryInstanceCoordinates {
            summary_definition_id: SummaryDefinitionId(PolicyFingerprint(7)),
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 10,
            },
            group_values: BTreeMap::from([("job".into(), "api".into())]),
        }
    }

    fn staged(epoch: u64) -> StagedSummaryInput {
        StagedSummaryInput {
            catalog_generation: generation(),
            dag_id: "dag".into(),
            consumer_node_id: "join".into(),
            input_node_id: "left".into(),
            source: source(epoch),
            instance_id: SummaryInstanceId::new(format!("instance-{epoch}")).unwrap(),
            coordinates: coordinates(),
            input_lineage: vec![epoch as u8],
            state_reference: state_reference(format!("state-{epoch}")),
        }
    }

    fn state_reference(key: String) -> SummaryStateReference {
        SummaryStateReference {
            store: "summary-store".into(),
            key,
            state_schema_version: 1,
            generation: 1,
            sequence: 1,
            checksum: Some("sha256:abc".into()),
        }
    }

    #[test]
    fn restart_recovers_staging_and_idempotence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination.json");
        let checkpoint_store = SummaryCoordinationCheckpointStore::open(&path).unwrap();
        assert!(checkpoint_store.stage_if_absent(staged(1)).unwrap());
        assert!(!checkpoint_store.stage_if_absent(staged(1)).unwrap());
        drop(checkpoint_store);
        let recovered = SummaryCoordinationCheckpointStore::open(&path).unwrap();
        assert_eq!(recovered.staged().unwrap(), vec![staged(1)]);
        assert!(!recovered.stage_if_absent(staged(1)).unwrap());
    }

    #[test]
    fn epochs_are_distinct_and_instance_id_equivocation_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint_store =
            SummaryCoordinationCheckpointStore::open(dir.path().join("checkpoint.json")).unwrap();
        assert!(checkpoint_store.stage_if_absent(staged(1)).unwrap());
        assert!(checkpoint_store.stage_if_absent(staged(2)).unwrap());
        let mut conflicting = staged(1);
        conflicting
            .coordinates
            .group_values
            .insert("job".into(), "other".into());
        assert!(checkpoint_store.stage_if_absent(conflicting).is_err());
    }

    #[test]
    fn watermark_rejects_regression_but_new_epoch_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint_store =
            SummaryCoordinationCheckpointStore::open(dir.path().join("checkpoint.json")).unwrap();
        let barrier = |epoch, sequence, watermark_ms| SummaryWatermarkBarrier {
            catalog_generation: generation(),
            source: source(epoch),
            sequence,
            watermark_ms,
        };
        assert!(checkpoint_store
            .advance_watermark(barrier(1, 2, 20))
            .unwrap());
        assert!(!checkpoint_store
            .advance_watermark(barrier(1, 2, 20))
            .unwrap());
        assert!(checkpoint_store
            .advance_watermark(barrier(1, 2, 21))
            .is_err());
        assert!(checkpoint_store
            .advance_watermark(barrier(1, 1, 30))
            .is_err());
        assert!(checkpoint_store
            .advance_watermark(barrier(2, 1, 5))
            .unwrap());
    }

    #[test]
    fn publication_key_is_durable_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let key = AtomicPublicationKey {
            catalog_generation: generation(),
            dag_id: "dag".into(),
            sink_node_id: "sink".into(),
            instance_id: SummaryInstanceId::new("output").unwrap(),
            coordinates: coordinates(),
            output_lineage: vec![1],
            state_reference: state_reference("output-state".into()),
        };
        let checkpoint_store = SummaryCoordinationCheckpointStore::open(&path).unwrap();
        assert!(checkpoint_store.publish_if_absent(key.clone()).unwrap());
        drop(checkpoint_store);
        let recovered = SummaryCoordinationCheckpointStore::open(&path).unwrap();
        assert!(!recovered.publish_if_absent(key.clone()).unwrap());
        assert!(
            SummaryCoordinationCheckpointStore::open(&path).is_err(),
            "writer lock remains held"
        );
        let mut conflicting = key.clone();
        conflicting.output_lineage = vec![2];
        assert!(recovered.publish_if_absent(conflicting).is_err());
        drop(recovered);
        assert!(!SummaryCoordinationCheckpointStore::open(&path)
            .unwrap()
            .publish_if_absent(key)
            .unwrap());
    }

    #[test]
    fn corrupt_or_unknown_wire_data_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        fs::write(&path, br#"{"schema_version":1,"revision":0,"staged":[],"watermarks":[],"published":[],"unknown":true}"#).unwrap();
        assert!(SummaryCoordinationCheckpointStore::open(path).is_err());
    }

    #[test]
    fn duplicate_primary_keys_on_disk_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let duplicate = staged(1);
        let document = CheckpointDocument {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            coordinator_scope: None,
            staged: vec![duplicate.clone(), duplicate],
            watermarks: Vec::new(),
            published: Vec::new(),
        };
        fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
        assert!(SummaryCoordinationCheckpointStore::open(path).is_err());
    }
}
