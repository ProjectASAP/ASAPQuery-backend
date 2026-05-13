//! Startup recovery: load the parts manifest, verify every referenced
//! part directory, and sweep orphan directories from
//! crashes-in-flight.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use tracing::{info, warn};

use super::flusher::parts_root;
use super::manifest::Manifest;
use super::part::{PartId, PartReader};
use super::PersistResult;

/// Result of a recovery pass.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub live_parts: usize,
    pub corrupt_parts_removed: usize,
    pub orphan_parts_removed: usize,
}

/// Open the manifest at `disk_path`, replay its log, validate every
/// referenced part, and sweep orphan part directories not in the
/// manifest.
pub fn recover(disk_path: &Path) -> PersistResult<(Manifest, RecoveryReport)> {
    fs::create_dir_all(disk_path)?;
    let parts_root = parts_root(disk_path);
    fs::create_dir_all(&parts_root)?;

    let manifest = Manifest::open_or_init(disk_path)?;
    let mut report = RecoveryReport::default();

    // Validate every live part by reading its meta.bin header.
    let mut to_drop: Vec<PartId> = Vec::new();
    let mut referenced: HashSet<PartId> = HashSet::new();
    for entry in manifest.live_parts() {
        referenced.insert(entry.part_id);
        let part_dir = super::part::part_dir_path(&parts_root, entry.part_id);
        match PartReader::read_meta(&part_dir) {
            Ok(meta) if meta.part_id == entry.part_id => {
                // Meta header checks out; don't also bother reading
                // data/index here — the query path will catch any
                // deeper corruption and surface it.
            }
            Ok(meta) => {
                warn!(
                    "recovery: part_id mismatch in meta.bin at {:?}: header says {}, manifest says {}",
                    part_dir, meta.part_id, entry.part_id
                );
                to_drop.push(entry.part_id);
            }
            Err(e) => {
                warn!(
                    "recovery: unreadable part {} at {:?}: {}; dropping from manifest",
                    entry.part_id, part_dir, e
                );
                to_drop.push(entry.part_id);
            }
        }
    }

    for part_id in to_drop {
        manifest.append_delete(part_id)?;
        let dir = super::part::part_dir_path(&parts_root, part_id);
        let _ = fs::remove_dir_all(&dir);
        report.corrupt_parts_removed += 1;
    }

    // Sweep orphans: any directory under parts/ whose parsed ID is not
    // in `referenced` is a leftover from a mid-flush crash.
    if parts_root.is_dir() {
        for entry in fs::read_dir(&parts_root)? {
            let entry = entry?;
            let name = match entry.file_name().to_str() {
                Some(s) => s.to_string(),
                None => continue,
            };
            let part_id = match u64::from_str_radix(&name, 16) {
                Ok(id) => id,
                Err(_) => continue,
            };
            if !referenced.contains(&part_id) {
                let path = entry.path();
                if let Err(e) = fs::remove_dir_all(&path) {
                    warn!(
                        "recovery: failed to remove orphan part dir {:?}: {}",
                        path, e
                    );
                } else {
                    report.orphan_parts_removed += 1;
                }
            }
        }
    }

    // Refresh manifest's in-memory view after dropping corrupt entries.
    let manifest = if report.corrupt_parts_removed > 0 {
        // Re-open so the in-memory snapshot is consistent with the
        // disk after our appends.
        Manifest::open_or_init(disk_path)?
    } else {
        manifest
    };

    report.live_parts = manifest.live_parts().len();

    info!(
        "persistence recovery: live={} corrupt_removed={} orphans_removed={}",
        report.live_parts, report.corrupt_parts_removed, report.orphan_parts_removed
    );

    Ok((manifest, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::types::KeyByLabelValues;
    use crate::storage_engines::sketch_db::index::persistence::part::{
        part_dir_path, PartWriter,
    };
    use crate::storage_engines::sketch_db::index::persistence::source::{
        EpochSnapshot, EpochSnapshotEntry,
    };
    use tempfile::TempDir;

    fn dummy_snapshot() -> EpochSnapshot {
        EpochSnapshot {
            agg_id: 1,
            epoch_id: 1,
            min_ts: 100,
            max_ts: 200,
            approx_bytes: 64,
            entries: vec![EpochSnapshotEntry {
                start_ts: 100,
                end_ts: 200,
                label: Some(KeyByLabelValues::new_with_labels(vec!["x".into()])),
                sketch_type_name: "SumAccumulator".into(),
                sketch_bytes: b"payload".to_vec(),
            }],
        }
    }

    #[test]
    fn recover_initializes_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let (m, report) = recover(tmp.path()).unwrap();
        assert_eq!(m.live_parts().len(), 0);
        assert_eq!(report.live_parts, 0);
        assert_eq!(report.corrupt_parts_removed, 0);
        assert_eq!(report.orphan_parts_removed, 0);
    }

    #[test]
    fn recover_sweeps_orphan_part_dir() {
        let tmp = TempDir::new().unwrap();
        // First: init an empty manifest so the directory exists.
        let _ = recover(tmp.path()).unwrap();

        // Now write a part on disk but do NOT reference it in the
        // manifest — simulates a crash between segment fsync and log
        // append.
        let parts_root = parts_root(tmp.path());
        std::fs::create_dir_all(&parts_root).unwrap();
        let orphan_dir = part_dir_path(&parts_root, 999);
        PartWriter::write_part(&orphan_dir, 999, &[dummy_snapshot()]).unwrap();
        assert!(orphan_dir.exists());

        let (_, report) = recover(tmp.path()).unwrap();
        assert_eq!(report.orphan_parts_removed, 1);
        assert!(!orphan_dir.exists());
    }

    #[test]
    fn recover_drops_corrupt_part_and_cleans_directory() {
        let tmp = TempDir::new().unwrap();
        // Init and write a legit part, then register it in the manifest.
        let (manifest, _) = recover(tmp.path()).unwrap();
        let parts_root = parts_root(tmp.path());
        let part_dir = part_dir_path(&parts_root, 42);
        let report_write = PartWriter::write_part(&part_dir, 42, &[dummy_snapshot()]).unwrap();
        manifest
            .append_add(
                crate::storage_engines::sketch_db::index::persistence::manifest::PartEntry {
                    part_id: 42,
                    min_ts: report_write.min_ts,
                    max_ts: report_write.max_ts,
                    size_bytes: report_write.data_len + report_write.index_len,
                },
            )
            .unwrap();
        drop(manifest);

        // Corrupt meta.bin.
        let mut bytes = std::fs::read(part_dir.join("meta.bin")).unwrap();
        bytes[5] ^= 0xFF;
        std::fs::write(part_dir.join("meta.bin"), &bytes).unwrap();

        let (m, report) = recover(tmp.path()).unwrap();
        assert_eq!(report.corrupt_parts_removed, 1);
        assert_eq!(m.live_parts().len(), 0);
        assert!(!part_dir.exists());
    }
}
