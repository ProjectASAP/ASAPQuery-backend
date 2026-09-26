//! Crash-resumable publication of one immutable output window. The SID sidecar
//! reserves the existing part ID before writing; no second payload store is used.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::flusher::{FlusherHandle, FlusherShared};
use super::manifest::PartEntry;
use super::metadata::StoredOutputMetadataRecord;
use super::part::{part_dir_path, PartReader, PartWriter};
use super::source::{EpochSnapshot, EpochSnapshotEntry};
use super::{PersistError, PersistResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImmutableOutputReservation {
    pub part_id: u64,
    pub input_digest: [u8; 32],
    pub payload_digest: [u8; 32],
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableWindowPublication {
    pub part_id: u64,
    pub already_published: bool,
}

fn invalid(message: &str) -> PersistError {
    PersistError::Format(message.into())
}

fn validate_identity(
    current: &StoredOutputMetadataRecord,
    record: &StoredOutputMetadataRecord,
) -> PersistResult<()> {
    if current.storage_handle != record.storage_handle
        || current.removed
        || current.retired_at_ms.is_some()
        || current.expires_at_ms.is_some()
        || current.stored_output_reference != record.stored_output_reference
        || current.summary_definition_id != record.summary_definition_id
        || current.catalog_generation != record.catalog_generation
        || current.metric_name != record.metric_name
        || current.group_by_keys != record.group_by_keys
        || !current.same_kind(record)
    {
        return Err(invalid("immutable publication metadata identity changed"));
    }
    Ok(())
}

fn fingerprint(snapshot: &EpochSnapshot) -> PersistResult<[u8; 32]> {
    if snapshot.entries.is_empty() || snapshot.min_ts >= snapshot.max_ts {
        return Err(invalid("immutable publication requires a nonempty window"));
    }
    let mut rows = Vec::with_capacity(snapshot.entries.len());
    for entry in &snapshot.entries {
        if entry.start_ts != snapshot.min_ts || entry.end_ts != snapshot.max_ts {
            return Err(invalid("immutable publication spans multiple windows"));
        }
        let label = entry.label.as_ref().map(|label| label.serialize_to_bytes());
        rows.push((
            label,
            &entry.sketch_type_name,
            entry.encoding_tag,
            &entry.sketch_bytes,
        ));
    }
    rows.sort();
    if rows.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid(
            "immutable publication contains duplicate label populations",
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"asap-immutable-window-v1");
    for value in [
        snapshot.agg_id,
        snapshot.min_ts,
        snapshot.max_ts,
        rows.len() as u64,
    ] {
        digest.update(value.to_le_bytes());
    }
    for (label, kind, encoding, payload) in rows {
        digest.update([u8::from(label.is_some())]);
        let label = label.as_deref().unwrap_or_default();
        for bytes in [label, kind.as_bytes()] {
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(bytes);
        }
        digest.update([encoding]);
        digest.update((payload.len() as u64).to_le_bytes());
        digest.update(payload);
    }
    Ok(digest.finalize().into())
}

impl FlusherHandle {
    /// Reuse the existing persistence state without transferring ownership of
    /// its worker thread. The store can retain a Weak reference to this Arc.
    pub(crate) fn publication_handle(&self) -> std::sync::Arc<FlusherShared> {
        std::sync::Arc::clone(&self.inner)
    }
    pub fn publish_immutable_window(
        &self,
        record: &StoredOutputMetadataRecord,
        input_digest: [u8; 32],
        snapshot: &EpochSnapshot,
    ) -> PersistResult<ImmutableWindowPublication> {
        self.inner
            .publish_immutable_window(record, input_digest, snapshot)
    }
    pub fn resume_pending_immutable_window(
        &self,
        sid: u64,
    ) -> PersistResult<Option<ImmutableWindowPublication>> {
        self.inner.resume_pending_immutable_window(sid)
    }
}

impl FlusherShared {
    /// Find the latest committed result before evaluating a potentially
    /// randomized sketch again. This never creates or replaces a payload.
    pub fn lookup_immutable_window(
        &self,
        record: &StoredOutputMetadataRecord,
        input_digest: [u8; 32],
        start_ms: u64,
        end_ms: u64,
    ) -> PersistResult<Option<ImmutableWindowPublication>> {
        self.sid_metadata.transaction(|records, _| {
            let Some(current) = records.get(&record.storage_handle.to_string()) else {
                return Ok(None);
            };
            validate_identity(current, record)?;
            let Some(previous) = &current.last_immutable else {
                return Ok(None);
            };
            if previous.start_ms != start_ms || previous.end_ms != end_ms {
                return Ok(None);
            }
            if previous.input_digest != input_digest {
                return Err(invalid("immutable lookup input lineage differs"));
            }
            self.validate_reserved_part(record.storage_handle, previous)?;
            if !self
                .manifest
                .live_parts()
                .iter()
                .any(|part| part.part_id == previous.part_id)
            {
                return Err(invalid("completed immutable part is no longer published"));
            }
            Ok(Some(ImmutableWindowPublication {
                part_id: previous.part_id,
                already_published: true,
            }))
        })
    }

    /// Publish a finalized window. A retry must present exactly the reserved
    /// input and payload. This does not guarantee source availability after GC.
    pub fn publish_immutable_window(
        &self,
        record: &StoredOutputMetadataRecord,
        input_digest: [u8; 32],
        snapshot: &EpochSnapshot,
    ) -> PersistResult<ImmutableWindowPublication> {
        if snapshot.agg_id != record.storage_handle || record.removed {
            return Err(invalid("immutable publication SID is invalid or removed"));
        }
        let payload_digest = fingerprint(snapshot)?;
        self.sid_metadata.transaction(|records, metadata| {
            let key = record.storage_handle.to_string();
            let mut current = records.get(&key).cloned().unwrap_or_else(|| record.clone());
            validate_identity(&current, record)?;
            // Completion fences window ends. An advancing overlapping full
            // window is a new immutable publication, not a correction.
            if current
                .completed_through_ms
                .is_some_and(|end| snapshot.max_ts <= end)
            {
                let Some(previous) = &current.last_immutable else {
                    return Err(invalid(
                        "completed immutable window has no retained lineage proof",
                    ));
                };
                if previous.input_digest != input_digest
                    || previous.payload_digest != payload_digest
                    || previous.start_ms != snapshot.min_ts
                    || previous.end_ms != snapshot.max_ts
                {
                    return Err(invalid(
                        "completed immutable retry differs from latest publication",
                    ));
                }
                self.validate_reserved_part(record.storage_handle, previous)?;
                if !self
                    .manifest
                    .live_parts()
                    .iter()
                    .any(|part| part.part_id == previous.part_id)
                {
                    return Err(invalid("completed immutable part is no longer published"));
                }
                return Ok(ImmutableWindowPublication {
                    part_id: previous.part_id,
                    already_published: true,
                });
            }
            let reservation = match current.pending_immutable.clone() {
                Some(pending) => {
                    if pending.input_digest != input_digest
                        || pending.payload_digest != payload_digest
                        || pending.start_ms != snapshot.min_ts
                        || pending.end_ms != snapshot.max_ts
                    {
                        return Err(invalid(
                            "another immutable publication is pending for this SID",
                        ));
                    }
                    pending
                }
                None => {
                    let pending = ImmutableOutputReservation {
                        part_id: self.allocate_part_id()?,
                        input_digest,
                        payload_digest,
                        start_ms: snapshot.min_ts,
                        end_ms: snapshot.max_ts,
                    };
                    current.pending_immutable = Some(pending.clone());
                    records.insert(key.clone(), current.clone());
                    metadata.write_records(records)?;
                    pending
                }
            };
            let path = part_dir_path(&self.cfg.disk_path.join("parts"), reservation.part_id);
            if path.exists() {
                // Never overwrite a reserved part silently: callers can recover a
                // durable part without sources; damaged/partial parts fail closed.
                self.validate_reserved_part(record.storage_handle, &reservation)?;
            } else {
                PartWriter::write_part(&path, reservation.part_id, std::slice::from_ref(snapshot))?;
                std::fs::File::open(
                    path.parent()
                        .ok_or_else(|| invalid("missing parts parent"))?,
                )?
                .sync_all()?;
                self.validate_reserved_part(record.storage_handle, &reservation)?;
            }
            self.finish_reserved_part(&mut current, &reservation)?;
            records.insert(key, current);
            metadata.write_records(records)?;
            Ok(ImmutableWindowPublication {
                part_id: reservation.part_id,
                already_published: false,
            })
        })
    }

    /// Resume only the caller's pending lineage, atomically with metadata
    /// validation. A different pending input cannot be acknowledged by this call.
    pub fn resume_matching_immutable_window(
        &self,
        record: &StoredOutputMetadataRecord,
        input_digest: [u8; 32],
        start_ms: u64,
        end_ms: u64,
    ) -> PersistResult<Option<ImmutableWindowPublication>> {
        self.resume_immutable_window(
            record.storage_handle,
            Some((record, input_digest, start_ms, end_ms)),
        )
    }

    /// Complete a pending durable part without reconstructing its source input.
    /// A reservation whose part was never completed returns an error, preserving
    /// the reservation for an explicit recovery policy rather than losing data.
    pub fn resume_pending_immutable_window(
        &self,
        sid: u64,
    ) -> PersistResult<Option<ImmutableWindowPublication>> {
        self.resume_immutable_window(sid, None)
    }

    fn resume_immutable_window(
        &self,
        sid: u64,
        expected: Option<(&StoredOutputMetadataRecord, [u8; 32], u64, u64)>,
    ) -> PersistResult<Option<ImmutableWindowPublication>> {
        self.sid_metadata.transaction(|records, metadata| {
            let key = sid.to_string();
            let Some(mut current) = records.get(&key).cloned() else {
                return Ok(None);
            };
            let Some(pending) = current.pending_immutable.clone() else {
                return Ok(None);
            };
            if current.removed || current.retired_at_ms.is_some() || current.expires_at_ms.is_some()
            {
                return Err(invalid("pending immutable SID was removed"));
            }
            if let Some((record, digest, start, end)) = expected {
                validate_identity(&current, record)?;
                if pending.input_digest != digest
                    || pending.start_ms != start
                    || pending.end_ms != end
                {
                    return Err(invalid("pending immutable recovery lineage differs"));
                }
            }
            self.validate_reserved_part(sid, &pending)?;
            self.finish_reserved_part(&mut current, &pending)?;
            records.insert(key, current);
            metadata.write_records(records)?;
            Ok(Some(ImmutableWindowPublication {
                part_id: pending.part_id,
                already_published: true,
            }))
        })
    }

    fn validate_reserved_part(
        &self,
        sid: u64,
        pending: &ImmutableOutputReservation,
    ) -> PersistResult<()> {
        let reader = PartReader::open(&part_dir_path(
            &self.cfg.disk_path.join("parts"),
            pending.part_id,
        ))?;
        if reader.meta.part_id != pending.part_id {
            return Err(invalid("reserved part ID mismatch"));
        }
        let mut entries = Vec::new();
        for index in reader.index_records() {
            let row = reader.load_entry(&index)?;
            if row.agg_id != sid {
                return Err(invalid("reserved part contains another SID"));
            }
            entries.push(EpochSnapshotEntry {
                start_ts: row.start_ts,
                end_ts: row.end_ts,
                label: row.label,
                sketch_type_name: row.sketch_type_name,
                encoding_tag: row.encoding_tag,
                sketch_bytes: row.sketch_bytes,
            });
        }
        let snapshot = EpochSnapshot {
            agg_id: sid,
            epoch_id: 0,
            min_ts: pending.start_ms,
            max_ts: pending.end_ms,
            entries,
            approx_bytes: 0,
        };
        if fingerprint(&snapshot)? != pending.payload_digest {
            return Err(invalid("reserved immutable payload digest mismatch"));
        }
        Ok(())
    }

    fn finish_reserved_part(
        &self,
        current: &mut StoredOutputMetadataRecord,
        pending: &ImmutableOutputReservation,
    ) -> PersistResult<()> {
        let path = part_dir_path(&self.cfg.disk_path.join("parts"), pending.part_id);
        // The reservation may have been recovered before its parent-directory
        // entry was durable; make that durable before manifest publication too.
        std::fs::File::open(
            path.parent()
                .ok_or_else(|| invalid("missing parts parent"))?,
        )?
        .sync_all()?;
        if !self
            .manifest
            .live_parts()
            .iter()
            .any(|part| part.part_id == pending.part_id)
        {
            let size_bytes = std::fs::read_dir(&path)?.try_fold(0u64, |sum, entry| {
                let length = entry?.metadata()?.len();
                sum.checked_add(length)
                    .ok_or_else(|| std::io::Error::other("part size overflow"))
            })?;
            self.manifest.append_add(PartEntry {
                part_id: pending.part_id,
                min_ts: pending.start_ms,
                max_ts: pending.end_ms,
                size_bytes,
            })?;
        }
        current.completed_through_ms = Some(
            current
                .completed_through_ms
                .unwrap_or(0)
                .max(pending.end_ms),
        );
        current.last_immutable = Some(pending.clone());
        current.pending_immutable = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        config::SketchStorePersistenceConfig,
        manifest::Manifest,
        source::{EpochSource, SealedEpochRef},
    };
    use super::*;
    use crate::storage_engines::{sketch_db::data::AggKind, types::AggregationType};
    use std::sync::Arc;

    struct EmptySource;
    impl EpochSource for EmptySource {
        fn list_sealed_epochs(&self) -> Vec<SealedEpochRef> {
            vec![]
        }
        fn snapshot_sealed_epoch(&self, _: u64, _: u64) -> PersistResult<Option<EpochSnapshot>> {
            Ok(None)
        }
        fn evict_sealed_epoch(&self, _: u64, _: u64) {}
        fn approx_memory_bytes(&self) -> usize {
            0
        }
    }
    fn handle(path: &std::path::Path) -> FlusherHandle {
        let mut config = SketchStorePersistenceConfig::with_memory_limit(1024, path.into());
        config.hot_window_ms = None;
        config.delete_older_than_ms = None;
        FlusherHandle::start(
            config,
            Arc::new(Manifest::open_or_init(path).unwrap()),
            Arc::new(EmptySource),
        )
        .unwrap()
    }
    fn record() -> StoredOutputMetadataRecord {
        StoredOutputMetadataRecord::new(
            1,
            "derived".into(),
            vec![],
            &AggKind::ExactAgg {
                agg_type: AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            0,
        )
    }
    fn snapshot() -> EpochSnapshot {
        EpochSnapshot {
            agg_id: 1,
            epoch_id: 0,
            min_ts: 10,
            max_ts: 20,
            approx_bytes: 8,
            entries: vec![EpochSnapshotEntry {
                start_ts: 10,
                end_ts: 20,
                label: None,
                sketch_type_name: "SumAccumulator".into(),
                encoding_tag: 0,
                sketch_bytes: vec![1, 2, 3],
            }],
        }
    }
    fn reserve(handle: &FlusherHandle, snapshot: &EpochSnapshot) -> ImmutableOutputReservation {
        let pending = ImmutableOutputReservation {
            part_id: handle.inner.allocate_part_id().unwrap(),
            input_digest: [7; 32],
            payload_digest: fingerprint(snapshot).unwrap(),
            start_ms: 10,
            end_ms: 20,
        };
        handle
            .inner
            .sid_metadata
            .transaction(|records, metadata| {
                let mut record = record();
                record.pending_immutable = Some(pending.clone());
                records.insert("1".into(), record);
                metadata.write_records(records)
            })
            .unwrap();
        pending
    }
    #[test]
    fn completed_retry_and_concurrent_publications_do_not_duplicate_parts() {
        let temp = tempfile::tempdir().unwrap();
        let handle = handle(temp.path());
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        handle
                            .publish_immutable_window(&record(), [7; 32], &snapshot())
                            .unwrap()
                    })
                })
                .collect();
            let ids: Vec<_> = workers
                .into_iter()
                .map(|worker| worker.join().unwrap().part_id)
                .collect();
            assert!(ids.iter().all(|id| *id == ids[0]));
        });
        assert_eq!(handle.manifest().live_parts().len(), 1);
        assert!(handle
            .publish_immutable_window(&record(), [8; 32], &snapshot())
            .is_err());
        let metadata = handle.inner.sid_metadata.load_strict().unwrap();
        assert_eq!(metadata[0].completed_through_ms, Some(20));
        assert!(metadata[0].pending_immutable.is_none());
        let mut different = snapshot();
        different.entries[0].sketch_bytes.push(4);
        assert!(handle
            .publish_immutable_window(&record(), [7; 32], &different)
            .is_err());
        let part_id = handle.manifest().live_parts()[0].part_id;
        handle.manifest().append_delete(part_id).unwrap();
        assert!(handle
            .publish_immutable_window(&record(), [7; 32], &snapshot())
            .is_err());
    }
    #[test]
    fn overlapping_windows_advance_end_frontier_and_reject_old_corrections_after_restart() {
        // Completion fences window ends, allowing an explicitly planned full
        // window to overlap its predecessor without reopening that predecessor.
        fn window(start: u64, end: u64) -> EpochSnapshot {
            let mut state = snapshot();
            state.min_ts = start;
            state.max_ts = end;
            state.entries[0].start_ts = start;
            state.entries[0].end_ts = end;
            state
        }
        let temp = tempfile::tempdir().unwrap();
        let mut first = handle(temp.path());
        first
            .publish_immutable_window(&record(), [1; 32], &window(0, 60))
            .unwrap();
        let next = first
            .publish_immutable_window(&record(), [2; 32], &window(10, 70))
            .unwrap();
        assert!(!next.already_published);
        assert_eq!(first.manifest().live_parts().len(), 2);
        first.shutdown();
        drop(first);
        let mut restored = handle(temp.path());
        let retried = restored
            .publish_immutable_window(&record(), [2; 32], &window(10, 70))
            .unwrap();
        assert!(retried.already_published);
        assert_eq!(retried.part_id, next.part_id);
        for (start, end, digest) in [(0, 60, [1; 32]), (0, 70, [2; 32]), (10, 70, [3; 32])] {
            assert!(restored
                .publish_immutable_window(&record(), digest, &window(start, end))
                .is_err());
        }
        let mut changed_payload = window(10, 70);
        changed_payload.entries[0].sketch_bytes.push(9);
        assert!(restored
            .publish_immutable_window(&record(), [2; 32], &changed_payload)
            .is_err());
        assert_eq!(restored.manifest().live_parts().len(), 2);
        let metadata = restored.inner.sid_metadata.load_strict().unwrap();
        assert_eq!(metadata[0].completed_through_ms, Some(70));
        assert!(metadata[0].pending_immutable.is_none());
        restored.shutdown();
    }
    #[test]
    fn restart_preserves_reserved_part_and_resumes_without_source() {
        let temp = tempfile::tempdir().unwrap();
        let mut first = handle(temp.path());
        let state = snapshot();
        let pending = reserve(&first, &state);
        let path = part_dir_path(&temp.path().join("parts"), pending.part_id);
        PartWriter::write_part(&path, pending.part_id, &[state]).unwrap();
        first.shutdown();
        drop(first);
        let (_, recovery) = super::super::recovery::recover(temp.path()).unwrap();
        assert_eq!(recovery.orphan_parts_removed, 0);
        let resumed = handle(temp.path());
        assert!(resumed.inner.allocate_part_id().unwrap() > pending.part_id);
        let result = resumed.resume_pending_immutable_window(1).unwrap().unwrap();
        assert_eq!(result.part_id, pending.part_id);
        assert_eq!(resumed.manifest().live_parts().len(), 1);
        assert!(resumed
            .resume_pending_immutable_window(1)
            .unwrap()
            .is_none());
    }
    #[test]
    fn pending_missing_part_requires_source_and_stale_upsert_preserves_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let handle = handle(temp.path());
        let pending = reserve(&handle, &snapshot());
        handle.inner.sid_metadata.upsert_all(&[record()]).unwrap();
        assert_eq!(
            handle.inner.sid_metadata.load_strict().unwrap()[0].pending_immutable,
            Some(pending.clone())
        );
        assert!(handle.resume_pending_immutable_window(1).is_err());
        assert_eq!(
            handle
                .publish_immutable_window(&record(), [7; 32], &snapshot())
                .unwrap()
                .part_id,
            pending.part_id
        );
        handle.inner.sid_metadata.upsert_all(&[record()]).unwrap();
        assert!(handle.inner.sid_metadata.load_strict().unwrap()[0]
            .pending_immutable
            .is_none());
    }
    #[test]
    fn malformed_metadata_fails_before_part_publication() {
        let temp = tempfile::tempdir().unwrap();
        let handle = handle(temp.path());
        std::fs::write(handle.inner.sid_metadata.path(), b"broken").unwrap();
        assert!(handle
            .publish_immutable_window(&record(), [7; 32], &snapshot())
            .is_err());
        assert!(handle.manifest().live_parts().is_empty());
    }
    #[test]
    fn failed_manifest_leaves_recoverable_payload_and_no_completion() {
        let temp = tempfile::tempdir().unwrap();
        let handle = handle(temp.path());
        let log = handle.manifest().log_path();
        let backup = log.with_extension("saved");
        std::fs::rename(&log, &backup).unwrap();
        std::fs::create_dir(&log).unwrap();
        assert!(handle
            .publish_immutable_window(&record(), [7; 32], &snapshot())
            .is_err());
        let records = handle.inner.sid_metadata.load_strict().unwrap();
        let pending = records[0].pending_immutable.clone().unwrap();
        assert_eq!(records[0].completed_through_ms, None);
        assert!(handle.manifest().live_parts().is_empty());
        std::fs::remove_dir(&log).unwrap();
        std::fs::rename(&backup, &log).unwrap();
        let published = handle.resume_pending_immutable_window(1).unwrap().unwrap();
        assert_eq!(published.part_id, pending.part_id);
        assert_eq!(handle.manifest().live_parts().len(), 1);
    }
    #[test]
    fn committed_lookup_reuses_payload_without_recomputing_randomized_state() {
        let temp = tempfile::tempdir().unwrap();
        let mut first = handle(temp.path());
        let publication = first
            .publish_immutable_window(&record(), [7; 32], &snapshot())
            .unwrap();
        first.shutdown();
        drop(first);
        let reopened = handle(temp.path());
        let found = reopened
            .inner
            .lookup_immutable_window(&record(), [7; 32], 10, 20)
            .unwrap()
            .unwrap();
        assert_eq!(found.part_id, publication.part_id);
        assert!(reopened
            .inner
            .lookup_immutable_window(&record(), [8; 32], 10, 20)
            .is_err());
        reopened
            .manifest()
            .append_delete(publication.part_id)
            .unwrap();
        assert!(reopened
            .inner
            .lookup_immutable_window(&record(), [7; 32], 10, 20)
            .is_err());
    }
    #[test]
    fn matching_resume_rejects_other_lineage_without_mutating_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let handle = handle(temp.path());
        let state = snapshot();
        let pending = reserve(&handle, &state);
        let path = part_dir_path(&temp.path().join("parts"), pending.part_id);
        PartWriter::write_part(&path, pending.part_id, &[state]).unwrap();
        assert!(handle
            .inner
            .resume_matching_immutable_window(&record(), [8; 32], 10, 20)
            .is_err());
        assert!(handle
            .inner
            .resume_matching_immutable_window(&record(), [7; 32], 20, 30)
            .is_err());
        assert!(handle.manifest().live_parts().is_empty());
        assert_eq!(
            handle.inner.sid_metadata.load_strict().unwrap()[0].pending_immutable,
            Some(pending.clone())
        );
        assert_eq!(
            handle
                .inner
                .resume_matching_immutable_window(&record(), [7; 32], 10, 20)
                .unwrap()
                .unwrap()
                .part_id,
            pending.part_id
        );
    }
}
