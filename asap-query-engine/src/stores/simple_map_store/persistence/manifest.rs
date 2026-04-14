//! Append-only parts manifest: `parts_manifest.log` + periodic
//! `parts_manifest.snapshot`. The manifest is the one piece of global
//! state on disk and the authoritative list of which parts are live.
//!
//! ## Binary layout
//!
//! Both files share a common record shape for add/delete operations:
//!
//! ```text
//! AddPart     : [u8 tag=1][u8 pad;7][u64 part_id][u64 min_ts][u64 max_ts][u64 size_bytes]
//! DeletePart  : [u8 tag=2][u8 pad;7][u64 part_id][u64 0][u64 0][u64 0]
//! ```
//!
//! 40 bytes per record, 8-byte aligned. The snapshot is simply a
//! sequence of `AddPart` records for the currently-live set, followed
//! by a u32 CRC trailer. The log is written by appending records, with
//! the same trailer rewritten atomically after each tick.
//!
//! The snapshot is rewritten (write → fsync → rename) whenever the log
//! has grown past some multiple of the live-set size. On recovery,
//! snapshot is loaded first, then the log is replayed from the start
//! since v1 does not yet track "log offset at snapshot time."

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use super::part::PartId;
use super::{PersistError, PersistResult};

pub const RECORD_SIZE: usize = 40;
pub const TAG_ADD: u8 = 1;
pub const TAG_DELETE: u8 = 2;
pub const SNAPSHOT_FILE: &str = "parts_manifest.snapshot";
pub const LOG_FILE: &str = "parts_manifest.log";

/// One live part, as seen by queries and the flusher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartEntry {
    pub part_id: PartId,
    pub min_ts: u64,
    pub max_ts: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum Record {
    Add(PartEntry),
    Delete(PartId),
}

fn encode_record(rec: Record, out: &mut [u8; RECORD_SIZE]) {
    for b in out.iter_mut() {
        *b = 0;
    }
    match rec {
        Record::Add(entry) => {
            out[0] = TAG_ADD;
            out[8..16].copy_from_slice(&entry.part_id.to_le_bytes());
            out[16..24].copy_from_slice(&entry.min_ts.to_le_bytes());
            out[24..32].copy_from_slice(&entry.max_ts.to_le_bytes());
            out[32..40].copy_from_slice(&entry.size_bytes.to_le_bytes());
        }
        Record::Delete(part_id) => {
            out[0] = TAG_DELETE;
            out[8..16].copy_from_slice(&part_id.to_le_bytes());
        }
    }
}

fn decode_record(bytes: &[u8; RECORD_SIZE]) -> PersistResult<Record> {
    let tag = bytes[0];
    let part_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    match tag {
        TAG_ADD => {
            let min_ts = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
            let max_ts = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
            let size_bytes = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
            Ok(Record::Add(PartEntry {
                part_id,
                min_ts,
                max_ts,
                size_bytes,
            }))
        }
        TAG_DELETE => Ok(Record::Delete(part_id)),
        other => Err(PersistError::Manifest(format!(
            "unknown manifest record tag: {}",
            other
        ))),
    }
}

/// The authoritative in-memory list of live parts plus the on-disk
/// log file handle. Cheap to clone via `Arc`.
pub struct Manifest {
    disk_path: PathBuf,
    live: Arc<RwLock<Vec<PartEntry>>>,
    /// Number of log records since the last snapshot. When this grows
    /// past `live.len() * 4`, we rewrite the snapshot and truncate the
    /// log. The threshold is arbitrary; re-tune if it bites.
    log_records_since_snapshot: std::sync::Mutex<u64>,
}

impl Manifest {
    /// Create an empty, fresh on-disk manifest in `disk_path`. Errors
    /// if `disk_path` already contains a snapshot or a non-empty log —
    /// callers should use [`Manifest::open_or_init`] for the common
    /// path.
    pub fn init(disk_path: &Path) -> PersistResult<Self> {
        fs::create_dir_all(disk_path)?;
        let snapshot_path = disk_path.join(SNAPSHOT_FILE);
        let log_path = disk_path.join(LOG_FILE);
        if snapshot_path.exists() || log_path.exists() {
            return Err(PersistError::Manifest(format!(
                "manifest already initialized at {:?}",
                disk_path
            )));
        }

        write_snapshot_atomic(&snapshot_path, &[])?;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&log_path)?;

        Ok(Self {
            disk_path: disk_path.to_path_buf(),
            live: Arc::new(RwLock::new(Vec::new())),
            log_records_since_snapshot: std::sync::Mutex::new(0),
        })
    }

    /// Load an existing manifest from `disk_path`. Initializes one if
    /// the directory has no snapshot yet.
    pub fn open_or_init(disk_path: &Path) -> PersistResult<Self> {
        fs::create_dir_all(disk_path)?;
        let snapshot_path = disk_path.join(SNAPSHOT_FILE);
        let log_path = disk_path.join(LOG_FILE);
        if !snapshot_path.exists() && !log_path.exists() {
            return Self::init(disk_path);
        }

        // Load snapshot (if present) then replay the log.
        let mut live: Vec<PartEntry> = if snapshot_path.exists() {
            read_snapshot(&snapshot_path)?
        } else {
            Vec::new()
        };

        if log_path.exists() {
            let records = read_log(&log_path)?;
            for rec in records {
                apply_record(&mut live, rec);
            }
        } else {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&log_path)?;
        }

        // Keep `live` sorted by part_id for stable iteration.
        live.sort_unstable_by_key(|e| e.part_id);

        Ok(Self {
            disk_path: disk_path.to_path_buf(),
            live: Arc::new(RwLock::new(live)),
            log_records_since_snapshot: std::sync::Mutex::new(0),
        })
    }

    pub fn snapshot_path(&self) -> PathBuf {
        self.disk_path.join(SNAPSHOT_FILE)
    }

    pub fn log_path(&self) -> PathBuf {
        self.disk_path.join(LOG_FILE)
    }

    /// A clone-on-read snapshot of the live parts list. Cheap —
    /// returns an owned `Vec` but the entries themselves are `Copy`.
    pub fn live_parts(&self) -> Vec<PartEntry> {
        self.live.read().unwrap().clone()
    }

    /// Parts whose `[min_ts, max_ts]` overlaps `[query_start,
    /// query_end]`. Linear scan — the manifest is tiny in v1.
    pub fn live_parts_overlapping(&self, query_start: u64, query_end: u64) -> Vec<PartEntry> {
        self.live
            .read()
            .unwrap()
            .iter()
            .filter(|p| !(p.max_ts < query_start || p.min_ts > query_end))
            .copied()
            .collect()
    }

    /// Append an add-part record to the log and make it durable.
    /// Updates in-memory state first, then writes to disk, then fsyncs.
    /// The caller must have already fsync'd the part's own files.
    pub fn append_add(&self, entry: PartEntry) -> PersistResult<()> {
        {
            let mut live = self.live.write().unwrap();
            live.push(entry);
            live.sort_unstable_by_key(|e| e.part_id);
        }
        self.append_record(Record::Add(entry))
    }

    /// Append a delete-part record. In-memory removal happens before
    /// disk write, same as add.
    pub fn append_delete(&self, part_id: PartId) -> PersistResult<()> {
        {
            let mut live = self.live.write().unwrap();
            live.retain(|e| e.part_id != part_id);
        }
        self.append_record(Record::Delete(part_id))
    }

    fn append_record(&self, rec: Record) -> PersistResult<()> {
        let mut buf = [0u8; RECORD_SIZE];
        encode_record(rec, &mut buf);
        {
            let mut f = OpenOptions::new().append(true).open(self.log_path())?;
            f.write_all(&buf)?;
            f.sync_data()?;
        }
        // fsync the parent directory for the log-file size update.
        if let Ok(dir) = File::open(&self.disk_path) {
            let _ = dir.sync_all();
        }

        // Maybe compact the log into a fresh snapshot.
        let mut counter = self.log_records_since_snapshot.lock().unwrap();
        *counter += 1;
        let live_len = self.live.read().unwrap().len() as u64;
        let threshold = live_len.saturating_mul(4).max(64);
        if *counter >= threshold {
            drop(counter);
            self.compact()?;
            let mut counter = self.log_records_since_snapshot.lock().unwrap();
            *counter = 0;
        }
        Ok(())
    }

    /// Rewrite the snapshot from the current in-memory live set and
    /// truncate the log. Atomic: the new snapshot goes to a tmp file
    /// first, then rename, then the log is truncated.
    pub fn compact(&self) -> PersistResult<()> {
        let live = self.live.read().unwrap().clone();
        write_snapshot_atomic(&self.snapshot_path(), &live)?;
        // Truncate log.
        let log_path = self.log_path();
        let f = OpenOptions::new().write(true).truncate(true).open(&log_path)?;
        f.sync_all()?;
        if let Ok(dir) = File::open(&self.disk_path) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

fn apply_record(live: &mut Vec<PartEntry>, rec: Record) {
    match rec {
        Record::Add(entry) => {
            if !live.iter().any(|e| e.part_id == entry.part_id) {
                live.push(entry);
            }
        }
        Record::Delete(part_id) => {
            live.retain(|e| e.part_id != part_id);
        }
    }
}

fn write_snapshot_atomic(path: &Path, live: &[PartEntry]) -> PersistResult<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut crc = crc32fast::Hasher::new();
        for entry in live {
            let mut buf = [0u8; RECORD_SIZE];
            encode_record(Record::Add(*entry), &mut buf);
            f.write_all(&buf)?;
            crc.update(&buf);
        }
        let crc_val = crc.finalize();
        f.write_all(&crc_val.to_le_bytes())?;
        f.write_all(&[0u8; 4])?;
        f.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn read_snapshot(path: &Path) -> PersistResult<Vec<PartEntry>> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    if buf.len() < 8 {
        return Ok(Vec::new());
    }
    let body_len = buf.len() - 8;
    if body_len % RECORD_SIZE != 0 {
        return Err(PersistError::Manifest(format!(
            "snapshot body misaligned: {} bytes",
            body_len
        )));
    }
    let crc_expected = u32::from_le_bytes(buf[body_len..body_len + 4].try_into().unwrap());
    let crc_actual = crc32fast::hash(&buf[..body_len]);
    if crc_expected != crc_actual {
        return Err(PersistError::Manifest(format!(
            "snapshot CRC mismatch: expected {:08x}, got {:08x}",
            crc_expected, crc_actual
        )));
    }
    let mut out = Vec::with_capacity(body_len / RECORD_SIZE);
    for chunk in buf[..body_len].chunks_exact(RECORD_SIZE) {
        let arr: [u8; RECORD_SIZE] = chunk.try_into().unwrap();
        match decode_record(&arr)? {
            Record::Add(entry) => out.push(entry),
            Record::Delete(_) => {
                return Err(PersistError::Manifest(
                    "snapshot contained a DELETE record".into(),
                ));
            }
        }
    }
    Ok(out)
}

fn read_log(path: &Path) -> PersistResult<Vec<Record>> {
    let mut f = File::open(path)?;
    let len = f.seek(SeekFrom::End(0))?;
    f.seek(SeekFrom::Start(0))?;
    if len == 0 {
        return Ok(Vec::new());
    }
    if len % RECORD_SIZE as u64 != 0 {
        // Tolerate trailing torn write — just truncate to the last
        // record boundary. Standard log-recovery practice.
        let good = (len / RECORD_SIZE as u64) * RECORD_SIZE as u64;
        let mut body = vec![0u8; good as usize];
        f.read_exact(&mut body)?;
        return decode_log_body(&body);
    }
    let mut body = vec![0u8; len as usize];
    f.read_exact(&mut body)?;
    decode_log_body(&body)
}

fn decode_log_body(body: &[u8]) -> PersistResult<Vec<Record>> {
    let mut out = Vec::with_capacity(body.len() / RECORD_SIZE);
    for chunk in body.chunks_exact(RECORD_SIZE) {
        let arr: [u8; RECORD_SIZE] = chunk.try_into().unwrap();
        out.push(decode_record(&arr)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn entry(part_id: u64, min_ts: u64, max_ts: u64) -> PartEntry {
        PartEntry {
            part_id,
            min_ts,
            max_ts,
            size_bytes: 1024,
        }
    }

    #[test]
    fn append_and_reload_round_trip() {
        let tmp = TempDir::new().unwrap();
        let m = Manifest::init(tmp.path()).unwrap();
        m.append_add(entry(1, 100, 200)).unwrap();
        m.append_add(entry(2, 200, 300)).unwrap();
        m.append_add(entry(3, 300, 400)).unwrap();
        m.append_delete(2).unwrap();

        // Drop and re-open — should see parts {1, 3}.
        drop(m);
        let m2 = Manifest::open_or_init(tmp.path()).unwrap();
        let live = m2.live_parts();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].part_id, 1);
        assert_eq!(live[1].part_id, 3);
    }

    #[test]
    fn compact_rewrites_snapshot_and_truncates_log() {
        let tmp = TempDir::new().unwrap();
        let m = Manifest::init(tmp.path()).unwrap();
        for i in 1..=10 {
            m.append_add(entry(i, i * 100, i * 100 + 50)).unwrap();
        }
        m.compact().unwrap();

        // Log file should be empty after compaction.
        let log_meta = std::fs::metadata(m.log_path()).unwrap();
        assert_eq!(log_meta.len(), 0);

        let m2 = Manifest::open_or_init(tmp.path()).unwrap();
        let live = m2.live_parts();
        assert_eq!(live.len(), 10);
        for (i, e) in live.iter().enumerate() {
            assert_eq!(e.part_id, i as u64 + 1);
        }
    }

    #[test]
    fn live_parts_overlapping_filters_correctly() {
        let tmp = TempDir::new().unwrap();
        let m = Manifest::init(tmp.path()).unwrap();
        m.append_add(entry(1, 100, 200)).unwrap();
        m.append_add(entry(2, 300, 400)).unwrap();
        m.append_add(entry(3, 500, 600)).unwrap();

        let hits = m.live_parts_overlapping(150, 350);
        let ids: Vec<u64> = hits.iter().map(|e| e.part_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
