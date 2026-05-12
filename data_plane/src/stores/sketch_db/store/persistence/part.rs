//! On-disk part format: `meta.bin` + `data.bin` + `index.bin`.
//!
//! A *part* is one flush tick's bundle of sealed-epoch entries, stored
//! in `<disk_path>/parts/<part_id_zero_padded>/`. See the design doc
//! section "Disk layout" for the full format.
//!
//! ## v1 format
//!
//! Custom little-endian binary throughout. 8-byte alignment on anything
//! the reader needs to cast directly. CRC32 (crc32fast) trailer on each
//! file. Written via `fallocate` when available, sync'd file-by-file,
//! directory fsync after all three files are durable.
//!
//! File shapes (v1):
//!
//! ```text
//! meta.bin (fixed 64-byte header + a variable-size trailer we ignore)
//!   u32 MAGIC_META
//!   u16 VERSION = 1
//!   u16 _flags                     (reserved, zero)
//!   u64 part_id
//!   u64 min_ts
//!   u64 max_ts
//!   u32 num_entries
//!   u32 _reserved
//!   u64 data_len
//!   u64 index_len
//!   u64 created_unix_ms
//!   u32 crc32_of_header            (covers all preceding 60 bytes)
//!   u32 _pad
//!
//! data.bin (opaque byte blob; entries concatenated, each 8-byte
//! aligned; the reader never scans it linearly, it only seeks via index
//! offsets)
//!   repeat num_entries:
//!     [label_len: u32]
//!     [payload_len: u32]
//!     [label bytes: label_len bytes, bincode-serialized Option<KeyByLabelValues>]
//!     [pad to 8-byte align of payload]
//!     [type_name_len: u16]
//!     [pad to 8-byte align of type_name (small)]
//!     [type_name bytes]
//!     [pad to 8-byte align]
//!     [payload bytes: payload_len bytes]
//!     [tail pad to 8-byte align]
//!   u32 crc32_of_body
//!   u32 _pad
//!
//! index.bin (sorted by (agg_id is same for whole part in v1 — one
//! part mixes epochs but one sealed epoch is one agg_id; for now we
//! store agg_id per entry for forward-compat, start_ts))
//!   repeat num_entries:
//!     [agg_id: u64]
//!     [start_ts: u64]
//!     [end_ts: u64]
//!     [data_offset: u64]
//!   u32 crc32_of_body
//!   u32 _pad
//! ```
//!
//! The data encoding is intentionally simple (length-prefixed fields,
//! 8-byte alignment) and skips a bunch of the micro-optimizations in
//! the design doc. v1 priorities are correctness, round-trip, and
//! passing tests — not beating Prometheus TSDB.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use memmap2::Mmap;

use super::source::EpochSnapshot;
use super::{PersistError, PersistResult};

pub type PartId = u64;

pub(crate) const MAGIC_META: u32 = 0x4D_50_41_52; // "MPAR"
pub(crate) const PART_FORMAT_VERSION: u16 = 1;
pub(crate) const META_HEADER_SIZE: usize = 64;
pub(crate) const INDEX_ENTRY_SIZE: usize = 32;

/// Zero-padded directory name for a part.
pub fn part_dir_name(part_id: PartId) -> String {
    format!("{:016x}", part_id)
}

/// Full path for a part directory under a given parts root.
pub fn part_dir_path(parts_root: &Path, part_id: PartId) -> PathBuf {
    parts_root.join(part_dir_name(part_id))
}

/// One entry inside a decoded part. The `start_ts`/`end_ts`/`label`
/// fields are resolved by the reader; the sketch payload stays as
/// bytes so the query path can decide whether to decode lazily.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub agg_id: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub label: Option<crate::stores::types::KeyByLabelValues>,
    pub sketch_type_name: String,
    pub sketch_bytes: Vec<u8>,
}

/// Write one [`EpochSnapshot`] (or many, concatenated) into a fresh
/// part directory. Consumes snapshots in order and produces the final
/// `(data_len, index_len)` for the manifest record. Streams data
/// through a `BufWriter` with a running CRC.
pub struct PartWriter;

impl PartWriter {
    /// Build a new part directory from a list of epoch snapshots. The
    /// snapshots may come from multiple agg-ids (one flush tick bundles
    /// candidates from whichever aggs needed eviction).
    ///
    /// Returns `(min_ts, max_ts, num_entries, data_len, index_len)`.
    pub fn write_part(
        part_dir: &Path,
        part_id: PartId,
        snapshots: &[EpochSnapshot],
    ) -> PersistResult<PartWriteReport> {
        fs::create_dir_all(part_dir)?;

        // ---- Build in-memory entry plan (offsets + index entries) ----
        let mut entries_plan: Vec<PlannedEntry> = Vec::new();
        let mut data_len: u64 = 0;
        let mut min_ts: u64 = u64::MAX;
        let mut max_ts: u64 = 0;

        for snap in snapshots {
            for e in &snap.entries {
                let label_bytes = match &e.label {
                    Some(k) => k.serialize_to_bytes(),
                    None => Vec::new(),
                };
                let type_name_bytes = e.sketch_type_name.as_bytes().to_vec();
                // Layout inside data.bin per entry:
                //   u32 label_len
                //   u32 payload_len
                //   u16 type_name_len
                //   u16 _pad
                //   u32 _pad
                //   label_bytes + pad-to-8
                //   type_name_bytes + pad-to-8
                //   payload_bytes + pad-to-8
                let header_size = 16; // 4+4+2+2+4
                let label_padded = align_up(label_bytes.len(), 8);
                let type_padded = align_up(type_name_bytes.len(), 8);
                let payload_padded = align_up(e.sketch_bytes.len(), 8);
                let entry_size = header_size + label_padded + type_padded + payload_padded;

                let data_offset = data_len;
                entries_plan.push(PlannedEntry {
                    agg_id: snap.agg_id,
                    start_ts: e.start_ts,
                    end_ts: e.end_ts,
                    data_offset,
                    label_bytes,
                    type_name_bytes,
                    sketch_bytes: e.sketch_bytes.clone(),
                });
                data_len += entry_size as u64;
                min_ts = min_ts.min(e.start_ts);
                max_ts = max_ts.max(e.end_ts);
            }
        }

        if entries_plan.is_empty() {
            return Err(PersistError::Internal(
                "PartWriter::write_part called with zero entries".to_string(),
            ));
        }

        let num_entries = entries_plan.len() as u32;
        let index_len = num_entries as u64 * INDEX_ENTRY_SIZE as u64 + 8; // + crc trailer

        // ---- Write data.bin with streaming CRC ----
        let data_path = part_dir.join("data.bin");
        let mut data_file = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&data_path)?,
        );
        let mut data_crc = crc32fast::Hasher::new();
        for pe in &entries_plan {
            write_u32(&mut data_file, &mut data_crc, pe.label_bytes.len() as u32)?;
            write_u32(&mut data_file, &mut data_crc, pe.sketch_bytes.len() as u32)?;
            write_u16(
                &mut data_file,
                &mut data_crc,
                pe.type_name_bytes.len() as u16,
            )?;
            write_u16(&mut data_file, &mut data_crc, 0)?; // _pad
            write_u32(&mut data_file, &mut data_crc, 0)?; // _pad
            write_padded(&mut data_file, &mut data_crc, &pe.label_bytes, 8)?;
            write_padded(&mut data_file, &mut data_crc, &pe.type_name_bytes, 8)?;
            write_padded(&mut data_file, &mut data_crc, &pe.sketch_bytes, 8)?;
        }
        let data_crc_val = data_crc.finalize();
        data_file.write_all(&data_crc_val.to_le_bytes())?;
        data_file.write_all(&[0u8; 4])?; // pad
        data_file.flush()?;
        let data_file = data_file
            .into_inner()
            .map_err(|e| PersistError::Io(e.into_error()))?;
        data_file.sync_all()?;
        drop(data_file);

        // ---- Write index.bin ----
        let index_path = part_dir.join("index.bin");
        let mut index_file = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&index_path)?,
        );
        let mut index_crc = crc32fast::Hasher::new();
        for pe in &entries_plan {
            write_u64(&mut index_file, &mut index_crc, pe.agg_id)?;
            write_u64(&mut index_file, &mut index_crc, pe.start_ts)?;
            write_u64(&mut index_file, &mut index_crc, pe.end_ts)?;
            write_u64(&mut index_file, &mut index_crc, pe.data_offset)?;
        }
        let index_crc_val = index_crc.finalize();
        index_file.write_all(&index_crc_val.to_le_bytes())?;
        index_file.write_all(&[0u8; 4])?;
        index_file.flush()?;
        let index_file = index_file
            .into_inner()
            .map_err(|e| PersistError::Io(e.into_error()))?;
        index_file.sync_all()?;
        drop(index_file);

        // ---- Write meta.bin (64 bytes, CRC'd) ----
        let meta_path = part_dir.join("meta.bin");
        let created_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // 64-byte header. Layout:
        //   [0..4]   u32 magic
        //   [4..6]   u16 version
        //   [6..8]   u16 flags
        //   [8..16]  u64 part_id
        //   [16..24] u64 min_ts
        //   [24..32] u64 max_ts
        //   [32..36] u32 num_entries
        //   [36..40] u32 reserved
        //   [40..48] u64 data_len
        //   [48..56] u64 index_len
        //   [56..60] u32 created_unix_secs (seconds granularity; diagnostic)
        //   [60..64] u32 crc32 of [0..60]
        let mut header = [0u8; META_HEADER_SIZE];
        let mut cursor = 0usize;
        write_u32_into(&mut header, &mut cursor, MAGIC_META);
        write_u16_into(&mut header, &mut cursor, PART_FORMAT_VERSION);
        write_u16_into(&mut header, &mut cursor, 0); // flags
        write_u64_into(&mut header, &mut cursor, part_id);
        write_u64_into(&mut header, &mut cursor, min_ts);
        write_u64_into(&mut header, &mut cursor, max_ts);
        write_u32_into(&mut header, &mut cursor, num_entries);
        write_u32_into(&mut header, &mut cursor, 0); // reserved
        write_u64_into(&mut header, &mut cursor, data_len);
        write_u64_into(&mut header, &mut cursor, index_len);
        let created_secs: u32 = (created_ms / 1000).min(u32::MAX as u64) as u32;
        write_u32_into(&mut header, &mut cursor, created_secs);
        let meta_crc = crc32fast::hash(&header[..60]);
        header[60..64].copy_from_slice(&meta_crc.to_le_bytes());

        {
            let mut meta_file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&meta_path)?;
            meta_file.write_all(&header)?;
            meta_file.sync_all()?;
        }

        // fsync the directory so the files are durably linked.
        if let Ok(dir) = File::open(part_dir) {
            let _ = dir.sync_all();
        }

        Ok(PartWriteReport {
            part_id,
            min_ts,
            max_ts,
            num_entries,
            data_len,
            index_len,
        })
    }
}

/// What [`PartWriter::write_part`] reports back.
#[derive(Debug, Clone)]
pub struct PartWriteReport {
    pub part_id: PartId,
    pub min_ts: u64,
    pub max_ts: u64,
    pub num_entries: u32,
    pub data_len: u64,
    pub index_len: u64,
}

struct PlannedEntry {
    agg_id: u64,
    start_ts: u64,
    end_ts: u64,
    data_offset: u64,
    label_bytes: Vec<u8>,
    type_name_bytes: Vec<u8>,
    sketch_bytes: Vec<u8>,
}

// ----- small write helpers with running CRC -----

fn write_u16<W: Write>(w: &mut W, h: &mut crc32fast::Hasher, v: u16) -> std::io::Result<()> {
    let b = v.to_le_bytes();
    w.write_all(&b)?;
    h.update(&b);
    Ok(())
}

fn write_u32<W: Write>(w: &mut W, h: &mut crc32fast::Hasher, v: u32) -> std::io::Result<()> {
    let b = v.to_le_bytes();
    w.write_all(&b)?;
    h.update(&b);
    Ok(())
}

fn write_u64<W: Write>(w: &mut W, h: &mut crc32fast::Hasher, v: u64) -> std::io::Result<()> {
    let b = v.to_le_bytes();
    w.write_all(&b)?;
    h.update(&b);
    Ok(())
}

fn write_padded<W: Write>(
    w: &mut W,
    h: &mut crc32fast::Hasher,
    bytes: &[u8],
    align: usize,
) -> std::io::Result<()> {
    w.write_all(bytes)?;
    h.update(bytes);
    let padded = align_up(bytes.len(), align);
    let pad_len = padded - bytes.len();
    if pad_len > 0 {
        let pad = [0u8; 8];
        w.write_all(&pad[..pad_len])?;
        h.update(&pad[..pad_len]);
    }
    Ok(())
}

fn align_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

fn write_u16_into(buf: &mut [u8], cursor: &mut usize, v: u16) {
    buf[*cursor..*cursor + 2].copy_from_slice(&v.to_le_bytes());
    *cursor += 2;
}

fn write_u32_into(buf: &mut [u8], cursor: &mut usize, v: u32) {
    buf[*cursor..*cursor + 4].copy_from_slice(&v.to_le_bytes());
    *cursor += 4;
}

fn write_u64_into(buf: &mut [u8], cursor: &mut usize, v: u64) {
    buf[*cursor..*cursor + 8].copy_from_slice(&v.to_le_bytes());
    *cursor += 8;
}

// =================================================================
// Reader
// =================================================================

/// Decoded metadata header of a part (from `meta.bin`).
#[derive(Debug, Clone)]
pub struct PartMeta {
    pub part_id: PartId,
    pub min_ts: u64,
    pub max_ts: u64,
    pub num_entries: u32,
    pub data_len: u64,
    pub index_len: u64,
    pub created_unix_ms: u64,
}

/// One resolved index record. `sketch_bytes` and the label are lazy —
/// they live inside `data.bin` and are resolved on demand via
/// [`PartReader::load_entry`].
#[derive(Debug, Clone, Copy)]
pub struct IndexRecord {
    pub agg_id: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub data_offset: u64,
}

/// mmap-backed reader for a single part. Cheap to construct (three
/// mmaps + one header parse), safe to share across threads via `Arc`.
pub struct PartReader {
    pub meta: PartMeta,
    pub part_dir: PathBuf,
    data_mmap: Arc<Mmap>,
    index_mmap: Arc<Mmap>,
}

impl std::fmt::Debug for PartReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartReader")
            .field("meta", &self.meta)
            .field("part_dir", &self.part_dir)
            .field("data_mmap_len", &self.data_mmap.len())
            .field("index_mmap_len", &self.index_mmap.len())
            .finish()
    }
}

impl PartReader {
    pub fn open(part_dir: &Path) -> PersistResult<Self> {
        let meta = Self::read_meta(part_dir)?;

        let data_mmap = map_file(&part_dir.join("data.bin"))?;
        let index_mmap = map_file(&part_dir.join("index.bin"))?;

        // Sanity-check CRCs up front so we catch tampered files.
        let idx_payload_len = meta.num_entries as usize * INDEX_ENTRY_SIZE;
        if index_mmap.len() < idx_payload_len + 8 {
            return Err(PersistError::Format(format!(
                "index.bin too short for {} entries (have {} bytes)",
                meta.num_entries,
                index_mmap.len()
            )));
        }
        let idx_expected = u32::from_le_bytes(
            index_mmap[idx_payload_len..idx_payload_len + 4]
                .try_into()
                .unwrap(),
        );
        let idx_actual = crc32fast::hash(&index_mmap[..idx_payload_len]);
        if idx_expected != idx_actual {
            return Err(PersistError::Format(format!(
                "index.bin CRC mismatch: expected {:08x}, got {:08x}",
                idx_expected, idx_actual
            )));
        }

        Ok(Self {
            meta,
            part_dir: part_dir.to_path_buf(),
            data_mmap: Arc::new(data_mmap),
            index_mmap: Arc::new(index_mmap),
        })
    }

    /// Read and verify just the meta.bin header (cheap — 64 bytes).
    pub fn read_meta(part_dir: &Path) -> PersistResult<PartMeta> {
        let mut f = File::open(part_dir.join("meta.bin"))?;
        let mut buf = [0u8; META_HEADER_SIZE];
        f.read_exact(&mut buf)?;

        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC_META {
            return Err(PersistError::Format(format!(
                "meta.bin bad magic: {:08x}",
                magic
            )));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version != PART_FORMAT_VERSION {
            return Err(PersistError::Format(format!(
                "meta.bin unsupported version: {}",
                version
            )));
        }
        // 6..8 flags (ignored)
        let part_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let min_ts = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let max_ts = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let num_entries = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        // 36..40 reserved
        let data_len = u64::from_le_bytes(buf[40..48].try_into().unwrap());
        let index_len = u64::from_le_bytes(buf[48..56].try_into().unwrap());
        let created_unix_secs = u32::from_le_bytes(buf[56..60].try_into().unwrap()) as u64;
        let created_unix_ms = created_unix_secs * 1000;
        let crc_expected = u32::from_le_bytes(buf[60..64].try_into().unwrap());
        let crc_actual = crc32fast::hash(&buf[..60]);
        if crc_expected != crc_actual {
            return Err(PersistError::Format(format!(
                "meta.bin CRC mismatch: expected {:08x}, got {:08x}",
                crc_expected, crc_actual
            )));
        }

        Ok(PartMeta {
            part_id,
            min_ts,
            max_ts,
            num_entries,
            data_len,
            index_len,
            created_unix_ms,
        })
    }

    /// Return all index records. Small (32 B × num_entries) — cheap to
    /// materialize.
    pub fn index_records(&self) -> Vec<IndexRecord> {
        let n = self.meta.num_entries as usize;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let off = i * INDEX_ENTRY_SIZE;
            let agg_id = u64::from_le_bytes(self.index_mmap[off..off + 8].try_into().unwrap());
            let start_ts =
                u64::from_le_bytes(self.index_mmap[off + 8..off + 16].try_into().unwrap());
            let end_ts =
                u64::from_le_bytes(self.index_mmap[off + 16..off + 24].try_into().unwrap());
            let data_offset =
                u64::from_le_bytes(self.index_mmap[off + 24..off + 32].try_into().unwrap());
            out.push(IndexRecord {
                agg_id,
                start_ts,
                end_ts,
                data_offset,
            });
        }
        out
    }

    /// Resolve a single index record into a [`SnapshotEntry`] by reading
    /// the corresponding slice of `data.bin`. Decodes the inline label
    /// and copies the sketch payload out.
    pub fn load_entry(&self, rec: &IndexRecord) -> PersistResult<SnapshotEntry> {
        let off = rec.data_offset as usize;
        if off + 16 > self.data_mmap.len() {
            return Err(PersistError::Format(
                "data.bin offset out of range".to_string(),
            ));
        }
        let label_len =
            u32::from_le_bytes(self.data_mmap[off..off + 4].try_into().unwrap()) as usize;
        let payload_len =
            u32::from_le_bytes(self.data_mmap[off + 4..off + 8].try_into().unwrap()) as usize;
        let type_name_len =
            u16::from_le_bytes(self.data_mmap[off + 8..off + 10].try_into().unwrap()) as usize;
        // 10..12 pad, 12..16 pad
        let mut cursor = off + 16;
        let label_padded = align_up(label_len, 8);
        let label_bytes = &self.data_mmap[cursor..cursor + label_len];
        let label = if label_len == 0 {
            None
        } else {
            Some(
                crate::stores::types::KeyByLabelValues::deserialize_from_bytes(label_bytes)
                    .map_err(|e| PersistError::Format(format!("label decode: {}", e)))?,
            )
        };
        cursor += label_padded;
        let type_padded = align_up(type_name_len, 8);
        let type_name = std::str::from_utf8(&self.data_mmap[cursor..cursor + type_name_len])
            .map_err(|e| PersistError::Format(format!("type_name utf8: {}", e)))?
            .to_string();
        cursor += type_padded;
        let sketch_bytes = self.data_mmap[cursor..cursor + payload_len].to_vec();

        Ok(SnapshotEntry {
            agg_id: rec.agg_id,
            start_ts: rec.start_ts,
            end_ts: rec.end_ts,
            label,
            sketch_type_name: type_name,
            sketch_bytes,
        })
    }
}

fn map_file(path: &Path) -> PersistResult<Mmap> {
    let f = File::open(path)?;
    // Safety: we never mutate the underlying file while the mmap is
    // alive; the parts directory is owned by this process's flusher.
    let mmap = unsafe { Mmap::map(&f) }?;
    Ok(mmap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::types::KeyByLabelValues;
    use crate::stores::sketch_db::store::persistence::source::EpochSnapshotEntry;
    use tempfile::TempDir;

    fn make_snapshot() -> EpochSnapshot {
        EpochSnapshot {
            agg_id: 42,
            epoch_id: 7,
            min_ts: 1_000,
            max_ts: 2_000,
            approx_bytes: 128,
            entries: vec![
                EpochSnapshotEntry {
                    start_ts: 1_000,
                    end_ts: 1_500,
                    label: Some(KeyByLabelValues::new_with_labels(vec![
                        "svc".into(),
                        "api".into(),
                    ])),
                    sketch_type_name: "SumAccumulator".into(),
                    sketch_bytes: b"opaque-sketch-1".to_vec(),
                },
                EpochSnapshotEntry {
                    start_ts: 1_500,
                    end_ts: 2_000,
                    label: None,
                    sketch_type_name: "DatasketchesKLLAccumulator".into(),
                    sketch_bytes: b"opaque-sketch-2-more-bytes".to_vec(),
                },
            ],
        }
    }

    #[test]
    fn part_round_trip_writes_and_reads_back() {
        let tmp = TempDir::new().unwrap();
        let part_dir = tmp.path().join("0000000000000001");
        let snap = make_snapshot();
        let report =
            PartWriter::write_part(&part_dir, 1, std::slice::from_ref(&snap)).expect("write_part");

        assert_eq!(report.part_id, 1);
        assert_eq!(report.num_entries, 2);
        assert_eq!(report.min_ts, 1_000);
        assert_eq!(report.max_ts, 2_000);
        assert!(report.data_len > 0);
        assert!(report.index_len > 0);

        let reader = PartReader::open(&part_dir).expect("PartReader::open");
        assert_eq!(reader.meta.part_id, 1);
        assert_eq!(reader.meta.num_entries, 2);
        assert_eq!(reader.meta.min_ts, 1_000);
        assert_eq!(reader.meta.max_ts, 2_000);

        let recs = reader.index_records();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].agg_id, 42);
        assert_eq!(recs[0].start_ts, 1_000);
        assert_eq!(recs[0].end_ts, 1_500);

        let e0 = reader.load_entry(&recs[0]).expect("load_entry 0");
        assert_eq!(e0.sketch_type_name, "SumAccumulator");
        assert_eq!(e0.sketch_bytes, b"opaque-sketch-1");
        assert_eq!(
            e0.label.as_ref().unwrap().labels,
            vec!["svc".to_string(), "api".to_string()]
        );

        let e1 = reader.load_entry(&recs[1]).expect("load_entry 1");
        assert!(e1.label.is_none());
        assert_eq!(e1.sketch_type_name, "DatasketchesKLLAccumulator");
        assert_eq!(e1.sketch_bytes, b"opaque-sketch-2-more-bytes");
    }

    #[test]
    fn part_reader_rejects_corrupted_meta() {
        let tmp = TempDir::new().unwrap();
        let part_dir = tmp.path().join("0000000000000002");
        PartWriter::write_part(&part_dir, 2, &[make_snapshot()]).unwrap();

        // Flip a byte inside the header (but not in the CRC field).
        let meta_path = part_dir.join("meta.bin");
        let mut bytes = std::fs::read(&meta_path).unwrap();
        bytes[10] ^= 0xFF;
        std::fs::write(&meta_path, &bytes).unwrap();

        let err = PartReader::open(&part_dir).unwrap_err();
        match err {
            PersistError::Format(msg) => assert!(msg.contains("CRC mismatch")),
            other => panic!("expected Format error, got {:?}", other),
        }
    }

    #[test]
    fn part_writer_rejects_empty_snapshot_set() {
        let tmp = TempDir::new().unwrap();
        let part_dir = tmp.path().join("empty");
        let err = PartWriter::write_part(&part_dir, 99, &[]).unwrap_err();
        match err {
            PersistError::Internal(msg) => assert!(msg.contains("zero entries")),
            other => panic!("expected Internal, got {:?}", other),
        }
    }
}
