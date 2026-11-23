//! Materialized views on disk: one append-only file per refresh group,
//! `group-<index>.log`, one record per refreshed window holding its
//! `(start, end]` and the rows of each view the window was appended to,
//! synced to disk (`fdatasync`) every `view_sync_interval_ms` by the
//! persist thread that writes the file.
//! At startup the records still within each view's retention are loaded
//! back into the view store; open windows and raw samples are not
//! persisted.
//!
//! A file starts with a header naming the plan (a hash of the plan file)
//! and the group; a file written for another plan or group is renamed to
//! `*.stale` and never loaded. A record is a little-endian `u32` length
//! and a MessagePack payload; a torn or oversized record at the end (a
//! crash while writing), or the first record that does not decode and
//! everything after it, is cut off at load, so later records stay aligned
//! and read the text dictionary as it was written.
//!
//! A row's plain values are stored as the executor's `Value`s, an exact
//! state (sum, count, min, max, rate, increase) by its serde form, and a
//! sketch state by `asap_sketchlib`'s ASAPv1 bytes with the kernel it
//! rebuilds, through each kernel's `sketch()` and `restore_from_disk`. A
//! Hydra-Count-Min grid, shared by every group of a build, is stored once
//! per record. A UnivMon whose heaps mix key types has no ASAPv1 encoding,
//! so a view holding one is not persisted (counted in
//! `asap_view_windows_not_persisted_total`).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::summary_kernels::exact::ExactAccumulator;
use asap_executor::summary_kernels::univmon::UnivMonAccumulator;
use asap_executor::summary_kernels::weighted_frequency::{FrequencySketch, WeightedFrequency};
use asap_executor::summary_kernels::{
    CountMinSketchAccumulator, DDSketchAccumulator, DatasketchesKLLAccumulator, HllRegisters,
    HllSketchAccumulator, HydraCms, HydraCmsGroup, HydraKllSketchAccumulator, IncreaseAccumulator,
    KmvAccumulator,
};
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::AggregateCore;
use asap_sketchlib::sketches::hll::HyperLogLogImpl;
use asap_sketchlib::{
    Classic, CountMinF64, CountMinSketch, CountMinSketchWithHeap, CountSketch, CountSketchWithHeap,
    DdSketch, Hydra, Kll, Kmv, UnivMon,
};
use serde::{Deserialize, Serialize};

use crate::store::ViewStore;

/// A record longer than this is taken as torn: no window's rows are this
/// large, and a garbage length must not size an allocation.
const MAX_RECORD_BYTES: usize = 1 << 30;

/// The first record of a group file.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Header {
    /// A hash of the plan file the views were computed for.
    plan_hash: u64,
    group: usize,
}

/// The kernels whose sketch state is stored as ASAPv1 bytes, with what
/// rebuilding one needs beyond the bytes.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SketchKernel {
    DDSketch,
    Kll,
    CountMin,
    Kmv,
    HydraKll,
    /// HyperLogLog at a precision.
    Hll(u8),
    WeightedFrequency(FrequencyKind),
    UnivMon {
        max_key_bytes: usize,
        inserted_key_bytes: usize,
    },
}

/// Which sketch a weighted-frequency state keeps.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrequencyKind {
    Cms,
    CountSketch,
    CmsHeap,
    CountSketchHeap,
}

/// One stored value of a view row.
#[derive(Serialize, Deserialize)]
enum StoredValue {
    Plain(Value),
    /// A text value, by its index in the file's dictionary.
    Text(u32),
    Exact(ExactAccumulator),
    Increase(IncreaseAccumulator),
    Sketch {
        kernel: SketchKernel,
        #[serde(with = "serde_bytes")]
        asapv1: Vec<u8>,
    },
    /// A Hydra-Count-Min group: its group name, and its grid by index in
    /// the record's grids (one grid serves every group of a build).
    HydraCmsGroup {
        grid: u32,
        group: String,
    },
}

/// One refreshed window of a group: the rows of each view appended.
#[derive(Serialize, Deserialize)]
struct Record {
    start_ms: i64,
    end_ms: i64,
    /// Texts this record adds to the file's dictionary, in index order:
    /// label values and series identities are written once per file.
    #[serde(default)]
    new_texts: Vec<String>,
    /// The Hydra-Count-Min grids its rows read, as ASAPv1 bytes, each once.
    #[serde(default)]
    hydra_cms_grids: Vec<serde_bytes::ByteBuf>,
    views: Vec<(PhysicalASAPNodeId, Vec<Vec<StoredValue>>)>,
}

/// One view's stored rows of one window: `(start, end, rows)`.
type StoredWindow = (
    i64,
    i64,
    Vec<Vec<StoredValue>>,
    Arc<Vec<Option<Arc<HydraCms>>>>,
);

/// What persisting one window wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub bytes: u64,
    pub sync_ns: u64,
    /// Views whose window could not be encoded.
    pub not_persisted: u64,
}

/// A group file's text dictionary: each text's index, in the order the
/// records added them.
#[derive(Default)]
struct Dictionary {
    index: std::collections::HashMap<Arc<str>, u32>,
    len: u32,
}

/// The group files in one directory, for one plan.
pub struct ViewFiles {
    dir: PathBuf,
    plan_hash: u64,
    files: BTreeMap<usize, File>,
    dictionaries: BTreeMap<usize, Dictionary>,
    /// Files are synced at most this often; a window's record is durable
    /// within this long after it is written.
    sync_interval: Duration,
    last_sync: Instant,
    /// Groups written since the last sync.
    unsynced: std::collections::BTreeSet<usize>,
}

/// A stable 64-bit FNV-1a hash of `bytes`.
pub fn plan_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

impl ViewFiles {
    /// The group files in `dir` (created if missing) of the plan whose
    /// file hashes to `plan_hash`.
    pub fn open(dir: &Path, plan_hash: u64, sync_interval: Duration) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            plan_hash,
            files: BTreeMap::new(),
            dictionaries: BTreeMap::new(),
            sync_interval,
            last_sync: Instant::now(),
            unsynced: Default::default(),
        })
    }

    /// Splits the files into `shards` sets, refresh group `g` going to set
    /// `g % shards`, each with the dictionaries loaded for its groups: one
    /// writer per file, so records stay in window order.
    pub fn split(self, shards: usize) -> Vec<ViewFiles> {
        let shards = shards.max(1);
        let mut out: Vec<ViewFiles> = (0..shards)
            .map(|_| ViewFiles {
                dir: self.dir.clone(),
                plan_hash: self.plan_hash,
                files: BTreeMap::new(),
                dictionaries: BTreeMap::new(),
                sync_interval: self.sync_interval,
                last_sync: Instant::now(),
                unsynced: Default::default(),
            })
            .collect();
        for (group, file) in self.files {
            out[group % shards].files.insert(group, file);
        }
        for (group, dictionary) in self.dictionaries {
            out[group % shards].dictionaries.insert(group, dictionary);
        }
        out
    }

    /// How often the files are synced.
    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// Syncs every file written since the last sync when the sync interval
    /// has passed; returns the nanoseconds spent syncing.
    pub fn sync_if_due(&mut self) -> std::io::Result<u64> {
        if self.last_sync.elapsed() < self.sync_interval {
            return Ok(0);
        }
        self.sync()
    }

    /// Syncs every file written since the last sync; returns the
    /// nanoseconds spent.
    pub fn sync(&mut self) -> std::io::Result<u64> {
        let started = Instant::now();
        for group in std::mem::take(&mut self.unsynced) {
            if let Some(file) = self.files.get(&group) {
                file.sync_data()?;
            }
        }
        self.last_sync = Instant::now();
        Ok(started.elapsed().as_nanos() as u64)
    }

    fn path(&self, group: usize) -> PathBuf {
        self.dir.join(format!("group-{group}.log"))
    }

    /// Appends the rows each of `views` got for `(start, end]` as one record
    /// of `group`'s file, synced with the other files once the sync
    /// interval has passed. A view holding a state with no byte encoding is
    /// left out of the record and counted.
    pub fn append(
        &mut self,
        group: usize,
        (start_ms, end_ms): (i64, i64),
        views: &[(PhysicalASAPNodeId, &[Batch])],
    ) -> std::io::Result<Written> {
        let mut written = Written::default();
        let mut stored = Vec::new();
        let dictionary = self.dictionaries.entry(group).or_default();
        // Texts new to the file; added to its dictionary once written.
        let mut texts = Texts {
            dictionary,
            new: Vec::new(),
            new_index: std::collections::HashMap::new(),
            grids: Grids::default(),
        };
        for (view, batches) in views {
            let mark = texts.new.len();
            let grids_mark = texts.grids.bytes.len();
            match encode_rows(batches, &mut texts) {
                Some(rows) => stored.push((*view, rows)),
                None => {
                    texts.forget_after(mark);
                    texts.grids.forget_after(grids_mark);
                    written.not_persisted += 1;
                }
            }
        }
        if stored.is_empty() {
            return Ok(written);
        }
        let new_texts: Vec<Arc<str>> = texts.new;
        let record = Record {
            start_ms,
            end_ms,
            new_texts: new_texts.iter().map(|t| t.to_string()).collect(),
            hydra_cms_grids: texts
                .grids
                .bytes
                .into_iter()
                .map(serde_bytes::ByteBuf::from)
                .collect(),
            views: stored,
        };
        let file = match self.files.entry(group) {
            std::collections::btree_map::Entry::Occupied(file) => file.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let path = self.dir.join(format!("group-{group}.log"));
                let mut file = OpenOptions::new().create(true).append(true).open(path)?;
                if file.metadata()?.len() == 0 {
                    let header = Header {
                        plan_hash: self.plan_hash,
                        group,
                    };
                    file.write_all(&framed(&header)?)?;
                }
                entry.insert(file)
            }
        };
        let bytes = framed(&record)?;
        file.write_all(&bytes)?;
        let dictionary = self.dictionaries.entry(group).or_default();
        for text in new_texts {
            dictionary.index.insert(text, dictionary.len);
            dictionary.len += 1;
        }
        self.unsynced.insert(group);
        written.bytes = bytes.len() as u64;
        written.sync_ns = self.sync_if_due()?;
        Ok(written)
    }

    /// Loads into `views` the records of each group in `groups` (its views
    /// with their output schemas) that end within each view's retention of
    /// its newest record, oldest first; returns the (view, window) pairs
    /// loaded. A file of another plan or group is renamed `*.stale`; a torn
    /// record at a file's end is cut off; a record that does not decode, or
    /// is not after the rows already loaded, is skipped.
    pub fn load(
        &mut self,
        groups: &[Vec<(MaterializedView, SchemaRef)>],
        views: &mut ViewStore,
    ) -> std::io::Result<u64> {
        let mut loaded = 0;
        for (group, group_views) in groups.iter().enumerate() {
            let path = self.path(group);
            let Some(records) = self.read_group(&path, group)? else {
                continue;
            };
            // The file's dictionary, replayed in record order; appends
            // after the restart continue it.
            let mut texts: Vec<Arc<str>> = Vec::new();
            let mut by_view: BTreeMap<PhysicalASAPNodeId, Vec<StoredWindow>> = BTreeMap::new();
            for record in records {
                texts.extend(record.new_texts.iter().map(|t| Arc::from(t.as_str())));
                // A record's grids are rebuilt once and shared by its rows.
                let grids: Arc<Vec<Option<Arc<HydraCms>>>> = Arc::new(
                    record
                        .hydra_cms_grids
                        .iter()
                        .map(|bytes| {
                            let grid = Hydra::from_asapv1(bytes).ok()?;
                            HydraCms::restore_from_disk(grid).ok().map(Arc::new)
                        })
                        .collect(),
                );
                for (view, rows) in record.views {
                    by_view.entry(view).or_default().push((
                        record.start_ms,
                        record.end_ms,
                        rows,
                        grids.clone(),
                    ));
                }
            }
            let dictionary = self.dictionaries.entry(group).or_default();
            for text in &texts {
                dictionary.index.insert(text.clone(), dictionary.len);
                dictionary.len += 1;
            }
            for (view, schema) in group_views {
                let Some(own) = by_view.remove(&view.node_id) else {
                    continue;
                };
                let Some(newest_end) = own.iter().map(|r| r.1).max() else {
                    continue;
                };
                let oldest_kept_end = crate::store::oldest_kept_end(view, newest_end);
                for (start, end, rows, grids) in own {
                    if end <= oldest_kept_end && end != newest_end {
                        continue;
                    }
                    let Some(batch) = decode_rows(schema, rows, &texts, &grids) else {
                        tracing::warn!(
                            view = view.node_id,
                            start,
                            end,
                            "skipping a view record that does not decode"
                        );
                        continue;
                    };
                    if views.append(view, (start, end), vec![batch]).is_ok() {
                        loaded += 1;
                    }
                }
            }
        }
        Ok(loaded)
    }

    /// The records of `group`'s file, after cutting off a torn tail; `None`
    /// when there is no file or it belongs to another plan or group (it is
    /// then renamed `*.stale`).
    fn read_group(&self, path: &Path, group: usize) -> std::io::Result<Option<Vec<Record>>> {
        let Ok(file) = File::open(path) else {
            return Ok(None);
        };
        let frames = read_frames(file)?;
        let mut frames = frames.into_iter();
        let header_frame = frames.next();
        let header: Option<Header> = header_frame
            .as_ref()
            .and_then(|f| rmp_serde::from_slice(f).ok());
        let expected = Header {
            plan_hash: self.plan_hash,
            group,
        };
        if header.as_ref() != Some(&expected) {
            let stale = path.with_extension("log.stale");
            tracing::warn!(path = %path.display(), ?header, ?expected, "view file of another plan or group; renamed, not loaded");
            std::fs::rename(path, stale)?;
            return Ok(None);
        }
        // The records up to the first that does not decode: a later one
        // would read the per-file text dictionary shifted.
        let mut good_len = header_frame.map_or(0, |f| 4 + f.len() as u64);
        let mut records = Vec::new();
        for frame in frames {
            let Ok(record) = rmp_serde::from_slice(&frame) else {
                break;
            };
            records.push(record);
            good_len += 4 + frame.len() as u64;
        }
        let file = OpenOptions::new().write(true).open(path)?;
        if file.metadata()?.len() != good_len {
            tracing::warn!(path = %path.display(), good_len, "cutting off a torn record");
            file.set_len(good_len)?;
            file.sync_data()?;
        }
        Ok(Some(records))
    }
}

/// `value` as one record: its MessagePack length and bytes.
fn framed<T: Serialize>(value: &T) -> std::io::Result<Vec<u8>> {
    let payload = rmp_serde::to_vec(value).map_err(std::io::Error::other)?;
    let length = u32::try_from(payload.len())
        .ok()
        .filter(|&len| (len as usize) <= MAX_RECORD_BYTES)
        .ok_or_else(|| std::io::Error::other("view record too large"))?;
    let mut bytes = Vec::with_capacity(4 + payload.len());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// Every whole record of `file`'s, up to a torn one.
fn read_frames(file: File) -> std::io::Result<Vec<Vec<u8>>> {
    let mut reader = BufReader::new(file);
    let end = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?;
    let mut frames = Vec::new();
    let mut good_len = 0u64;
    loop {
        let mut length = [0u8; 4];
        if reader.read_exact(&mut length).is_err() {
            break;
        }
        let length = u32::from_le_bytes(length) as u64;
        if length as usize > MAX_RECORD_BYTES || good_len + 4 + length > end {
            break;
        }
        let mut payload = vec![0u8; length as usize];
        if reader.read_exact(&mut payload).is_err() {
            break;
        }
        frames.push(payload);
        good_len += 4 + length;
    }
    Ok(frames)
}

/// Each HyperLogLog precision's sketch type, from the executor's
/// `HllRegisters`.
macro_rules! hll_precisions {
    ($($variant:ident($storage:ident, $precision:literal)),* $(,)?) => {
        /// The precision and ASAPv1 bytes of an HLL state.
        fn encode_hll(registers: &HllRegisters) -> Option<(u8, Vec<u8>)> {
            match registers {
                $(HllRegisters::$variant(hll) => Some(($precision, hll.to_asapv1().ok()?)),)*
            }
        }

        /// The HLL state of `precision` the bytes encode.
        fn decode_hll(precision: u8, asapv1: &[u8]) -> Option<HllRegisters> {
            use asap_executor::summary_kernels::hll_sketch::hll_registers::*;
            match precision {
                $($precision => Some(HllRegisters::$variant(
                    HyperLogLogImpl::<Classic, $storage>::from_asapv1(asapv1).ok()?,
                )),)*
                _ => None,
            }
        }
    };
}

hll_precisions!(
    P4(HllRegistersP4, 4),
    P5(HllRegistersP5, 5),
    P6(HllRegistersP6, 6),
    P7(HllRegistersP7, 7),
    P8(HllRegistersP8, 8),
    P9(HllRegistersP9, 9),
    P10(HllRegistersP10, 10),
    P11(HllRegistersP11, 11),
    P12(HllRegistersP12, 12),
    P13(HllRegistersP13, 13),
    P14(HllRegistersP14, 14),
    P15(HllRegistersP15, 15),
    P16(HllRegistersP16, 16),
    P17(HllRegistersP17, 17),
    P18(HllRegistersP18, 18),
);

/// The Hydra-Count-Min grids of one record, each stored once.
#[derive(Default)]
struct Grids {
    bytes: Vec<Vec<u8>>,
    /// Each grid's index, by its address.
    index: std::collections::HashMap<usize, u32>,
}

impl Grids {
    /// `grid`'s index, its bytes stored on first sight; `None` when it does
    /// not encode.
    fn index(&mut self, grid: &Arc<HydraCms>) -> Option<u32> {
        let address = Arc::as_ptr(grid) as usize;
        if let Some(&index) = self.index.get(&address) {
            return Some(index);
        }
        let bytes = grid.sketch().to_asapv1().ok()?;
        let index = self.bytes.len() as u32;
        self.bytes.push(bytes);
        self.index.insert(address, index);
        Some(index)
    }

    /// Drops the grids after the first `mark` (a view left out).
    fn forget_after(&mut self, mark: usize) {
        self.bytes.truncate(mark);
        self.index.retain(|_, index| (*index as usize) < mark);
    }
}

/// `state` as stored bytes, when its kernel can be rebuilt from them; a
/// Hydra-Count-Min group's grid goes to `grids`.
fn encode_state(state: &dyn AggregateCore, grids: &mut Grids) -> Option<StoredValue> {
    let any = state.as_any();
    let sketch = |kernel, bytes: Option<Vec<u8>>| {
        Some(StoredValue::Sketch {
            kernel,
            asapv1: bytes?,
        })
    };
    if let Some(exact) = any.downcast_ref::<ExactAccumulator>() {
        Some(StoredValue::Exact(exact.clone()))
    } else if let Some(increase) = any.downcast_ref::<IncreaseAccumulator>() {
        Some(StoredValue::Increase(increase.clone()))
    } else if let Some(dd) = any.downcast_ref::<DDSketchAccumulator>() {
        sketch(SketchKernel::DDSketch, dd.sketch().to_asapv1().ok())
    } else if let Some(kll) = any.downcast_ref::<DatasketchesKLLAccumulator>() {
        sketch(SketchKernel::Kll, kll.sketch().to_asapv1().ok())
    } else if let Some(cms) = any.downcast_ref::<CountMinSketchAccumulator>() {
        sketch(SketchKernel::CountMin, cms.sketch().to_asapv1().ok())
    } else if let Some(kmv) = any.downcast_ref::<KmvAccumulator>() {
        sketch(SketchKernel::Kmv, kmv.sketch().to_asapv1().ok())
    } else if let Some(hydra) = any.downcast_ref::<HydraKllSketchAccumulator>() {
        sketch(SketchKernel::HydraKll, hydra.sketch().to_asapv1().ok())
    } else if let Some(hll) = any.downcast_ref::<HllSketchAccumulator>() {
        let (precision, bytes) = encode_hll(hll.sketch())?;
        sketch(SketchKernel::Hll(precision), Some(bytes))
    } else if let Some(frequency) = any.downcast_ref::<WeightedFrequency>() {
        let (kind, bytes) = match frequency.sketch() {
            FrequencySketch::Cms(s) => (FrequencyKind::Cms, s.to_asapv1().ok()?),
            FrequencySketch::CountSketch(s) => (FrequencyKind::CountSketch, s.to_asapv1().ok()?),
            FrequencySketch::CmsHeap(s) => (FrequencyKind::CmsHeap, s.to_asapv1().ok()?),
            FrequencySketch::CountSketchHeap(s) => {
                (FrequencyKind::CountSketchHeap, s.to_asapv1().ok()?)
            }
        };
        sketch(SketchKernel::WeightedFrequency(kind), Some(bytes))
    } else if let Some(univmon) = any.downcast_ref::<UnivMonAccumulator>() {
        // A UnivMon whose heaps mix key types has no ASAPv1 encoding.
        sketch(
            SketchKernel::UnivMon {
                max_key_bytes: univmon.max_key_bytes(),
                inserted_key_bytes: univmon.inserted_key_bytes(),
            },
            univmon.sketch().to_asapv1().ok(),
        )
    } else if let Some(group) = any.downcast_ref::<HydraCmsGroup>() {
        Some(StoredValue::HydraCmsGroup {
            grid: grids.index(&group.grid)?,
            group: group.group.clone(),
        })
    } else {
        None
    }
}

/// The kernel state `kernel` rebuilds from `asapv1`.
pub(crate) fn decode_sketch(kernel: SketchKernel, asapv1: &[u8]) -> Option<Arc<dyn AggregateCore>> {
    Some(match kernel {
        SketchKernel::DDSketch => Arc::new(
            DDSketchAccumulator::restore_from_disk(DdSketch::from_asapv1(asapv1).ok()?).ok()?,
        ),
        SketchKernel::Kll => Arc::new(
            DatasketchesKLLAccumulator::restore_from_disk(Kll::from_asapv1(asapv1).ok()?).ok()?,
        ),
        SketchKernel::CountMin => Arc::new(
            CountMinSketchAccumulator::restore_from_disk(CountMinF64::from_asapv1(asapv1).ok()?)
                .ok()?,
        ),
        SketchKernel::Kmv => {
            Arc::new(KmvAccumulator::restore_from_disk(Kmv::from_asapv1(asapv1).ok()?).ok()?)
        }
        SketchKernel::HydraKll => Arc::new(
            HydraKllSketchAccumulator::restore_from_disk(Hydra::from_asapv1(asapv1).ok()?).ok()?,
        ),
        SketchKernel::Hll(precision) => {
            Arc::new(HllSketchAccumulator::restore_from_disk(decode_hll(precision, asapv1)?).ok()?)
        }
        SketchKernel::WeightedFrequency(kind) => {
            let sketch = match kind {
                FrequencyKind::Cms => {
                    FrequencySketch::Cms(CountMinSketch::from_asapv1(asapv1).ok()?)
                }
                FrequencyKind::CountSketch => {
                    FrequencySketch::CountSketch(CountSketch::from_asapv1(asapv1).ok()?)
                }
                FrequencyKind::CmsHeap => {
                    FrequencySketch::CmsHeap(CountMinSketchWithHeap::from_asapv1(asapv1).ok()?)
                }
                FrequencyKind::CountSketchHeap => {
                    FrequencySketch::CountSketchHeap(CountSketchWithHeap::from_asapv1(asapv1).ok()?)
                }
            };
            Arc::new(WeightedFrequency::restore_from_disk(sketch).ok()?)
        }
        SketchKernel::UnivMon {
            max_key_bytes,
            inserted_key_bytes,
        } => Arc::new(
            UnivMonAccumulator::restore_from_disk(
                UnivMon::from_asapv1(asapv1).ok()?,
                max_key_bytes,
                inserted_key_bytes,
            )
            .ok()?,
        ),
    })
}

/// Texts being encoded into one record: the file's dictionary, and the
/// texts this record adds to it.
struct Texts<'a> {
    dictionary: &'a Dictionary,
    new: Vec<Arc<str>>,
    new_index: std::collections::HashMap<Arc<str>, u32>,
    grids: Grids,
}

impl Texts<'_> {
    /// `text`'s index, added to the record's new texts if the file has none.
    fn index(&mut self, text: &Arc<str>) -> u32 {
        if let Some(&index) = self.dictionary.index.get(text) {
            return index;
        }
        if let Some(&index) = self.new_index.get(text) {
            return index;
        }
        let index = self.dictionary.len + self.new.len() as u32;
        self.new.push(text.clone());
        self.new_index.insert(text.clone(), index);
        index
    }

    /// Drops the new texts after the first `mark` (a view left out).
    fn forget_after(&mut self, mark: usize) {
        for text in self.new.drain(mark..) {
            self.new_index.remove(&text);
        }
    }
}

/// The rows of `batches`, texts as dictionary indices; `None` when one
/// holds a state with no byte encoding. A window split by series is kept
/// as one batch per part; its rows are written in the order one run over
/// every series emits them (by their plain columns' values), so a window's
/// record is the same whichever way its run was split.
fn encode_rows(batches: &[Batch], texts: &mut Texts) -> Option<Vec<Vec<StoredValue>>> {
    let mut rows: Vec<Vec<Value>> = batches.iter().flat_map(Batch::to_rows).collect();
    if batches.len() > 1 {
        let plain: Vec<usize> = batches[0]
            .schema()
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                matches!(
                    field.dtype,
                    asap_executor::planner::ir::schema::FieldDataType::Plain(_)
                )
            })
            .map(|(column, _)| column)
            .collect();
        let mut keyed = rows
            .into_iter()
            .map(|row| {
                let key = plain
                    .iter()
                    .map(|&column| row[column].key())
                    .collect::<Result<Vec<_>, _>>()
                    .ok()?;
                Some((key, row))
            })
            .collect::<Option<Vec<_>>>()?;
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        rows = keyed.into_iter().map(|(_, row)| row).collect();
    }
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Summary { state, .. } => encode_state(state.as_ref(), &mut texts.grids),
                    Value::Utf8(text) => Some(StoredValue::Text(texts.index(&text))),
                    plain => Some(StoredValue::Plain(plain)),
                })
                .collect()
        })
        .collect()
}

/// `rows` as one batch of `schema`, each state of its field's family.
fn decode_rows(
    schema: &SchemaRef,
    rows: Vec<Vec<StoredValue>>,
    texts: &[Arc<str>],
    grids: &[Option<Arc<HydraCms>>],
) -> Option<Batch> {
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .zip(&schema.fields)
                .map(|(value, field)| {
                    let state: Arc<dyn AggregateCore> = match value {
                        StoredValue::Plain(value) => return Some(value),
                        StoredValue::Text(index) => {
                            return texts.get(index as usize).cloned().map(Value::Utf8)
                        }
                        StoredValue::Exact(state) => Arc::new(state),
                        StoredValue::Increase(state) => Arc::new(state),
                        StoredValue::Sketch { kernel, asapv1 } => decode_sketch(kernel, &asapv1)?,
                        StoredValue::HydraCmsGroup { grid, group } => Arc::new(HydraCmsGroup {
                            grid: grids.get(grid as usize)?.clone()?,
                            group,
                        }),
                    };
                    Some(Value::Summary {
                        family: field.dtype.clone(),
                        state,
                    })
                })
                .collect::<Option<Vec<_>>>()
        })
        .collect::<Option<Vec<_>>>()?;
    Batch::try_new(schema.clone(), rows).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_executor::planner::summary_type::SketchStatistic;
    use asap_executor::values::Value;

    /// `state` as ASAPv1 bytes with its kernel, if it is one of the sketch
    /// kernels persisted (Hydra-Count-Min groups aside, stored by grid).
    fn encode_sketch(state: &dyn AggregateCore) -> Option<(SketchKernel, Vec<u8>)> {
        match encode_state(state, &mut Grids::default())? {
            StoredValue::Sketch { kernel, asapv1 } => Some((kernel, asapv1)),
            _ => None,
        }
    }

    /// Two states of each persisted sketch kernel, built from different
    /// samples.
    type StatePair = (SketchKernel, Box<dyn AggregateCore>, Box<dyn AggregateCore>);

    fn states() -> Vec<StatePair> {
        let mut out: Vec<StatePair> = Vec::new();
        let empty_dd = DDSketchAccumulator::new(0.01).sketch().clone();
        let (mut a, mut b) = (empty_dd.clone(), empty_dd);
        let (mut c, mut d) = (
            DatasketchesKLLAccumulator::new(200),
            DatasketchesKLLAccumulator::new(200),
        );
        let (mut e, mut f) = (
            CountMinSketchAccumulator::new(3, 64).unwrap(),
            CountMinSketchAccumulator::new(3, 64).unwrap(),
        );
        let (mut g, mut h) = (KmvAccumulator::new(64), KmvAccumulator::new(64));
        let (mut i, mut j) = (
            HydraKllSketchAccumulator::new(2, 8, 50),
            HydraKllSketchAccumulator::new(2, 8, 50),
        );
        let (mut k, mut l) = (
            HllSketchAccumulator::new(12).unwrap(),
            HllSketchAccumulator::new(12).unwrap(),
        );
        use asap_executor::summary_kernels::weighted_frequency::FrequencyAlgorithm;
        let frequency = |algorithm, capacity| {
            (
                WeightedFrequency::new(algorithm, 64, 3, capacity).unwrap(),
                WeightedFrequency::new(algorithm, 64, 3, capacity).unwrap(),
            )
        };
        let mut frequencies = [
            (FrequencyKind::Cms, frequency(FrequencyAlgorithm::Cms, 0)),
            (
                FrequencyKind::CountSketch,
                frequency(FrequencyAlgorithm::CountSketch, 0),
            ),
            (
                FrequencyKind::CmsHeap,
                frequency(FrequencyAlgorithm::Cms, 4),
            ),
            (
                FrequencyKind::CountSketchHeap,
                frequency(FrequencyAlgorithm::CountSketch, 4),
            ),
        ];
        let (mut m, mut o) = (
            UnivMonAccumulator::new(8, 3, 64, 4).unwrap(),
            UnivMonAccumulator::new(8, 3, 64, 4).unwrap(),
        );
        for n in 0..500 {
            let v = (n * 37 % 1000) as f64 + 0.5;
            let (first, key) = (n % 2 == 0, [format!("k{}", n % 5)]);
            let text = [Value::Utf8(key[0].as_str().into())];
            if first {
                a.add(&v);
                c.update(v);
                e.update(&key[0], 1.0 + v);
                g.insert_sample(v);
                i.update(&key, v);
                k.update(&v.to_le_bytes());
                m.insert_sample(v % 50.0).unwrap();
            } else {
                b.add(&v);
                d.update(v);
                f.update(&key[0], 1.0 + v);
                h.insert_sample(v);
                j.update(&key, v);
                l.update(&v.to_le_bytes());
                o.insert_sample(v % 50.0).unwrap();
            }
            for (_, (x, y)) in &mut frequencies {
                if first { x } else { y }.update(&text, 1.0 + v).unwrap();
            }
        }
        let dd = |sketch| Box::new(DDSketchAccumulator::restore_from_disk(sketch).unwrap());
        out.push((SketchKernel::DDSketch, dd(a), dd(b)));
        out.push((SketchKernel::Kll, Box::new(c), Box::new(d)));
        out.push((SketchKernel::CountMin, Box::new(e), Box::new(f)));
        out.push((SketchKernel::Kmv, Box::new(g), Box::new(h)));
        out.push((SketchKernel::HydraKll, Box::new(i), Box::new(j)));
        out.push((SketchKernel::Hll(12), Box::new(k), Box::new(l)));
        for (kind, (x, y)) in frequencies {
            out.push((
                SketchKernel::WeightedFrequency(kind),
                Box::new(x),
                Box::new(y),
            ));
        }
        let univmon = SketchKernel::UnivMon {
            max_key_bytes: m.max_key_bytes(),
            inserted_key_bytes: m.inserted_key_bytes(),
        };
        out.push((univmon, Box::new(m), Box::new(o)));
        out
    }

    /// What can be read from a state of `kernel`, to compare two states.
    fn readout(kernel: SketchKernel, state: &dyn AggregateCore) -> Vec<f64> {
        let any = state.as_any();
        match kernel {
            SketchKernel::DDSketch | SketchKernel::Kll => [0.1, 0.5, 0.99]
                .iter()
                .map(|&q| state.estimate(&SketchStatistic::Quantile { q }).unwrap())
                .collect(),
            SketchKernel::CountMin => {
                let cms = any.downcast_ref::<CountMinSketchAccumulator>().unwrap();
                (0..5).map(|k| cms.query_key(&[format!("k{k}")])).collect()
            }
            SketchKernel::Kmv => vec![state.estimate(&SketchStatistic::Cardinality).unwrap()],
            SketchKernel::HydraKll => {
                let hydra = any.downcast_ref::<HydraKllSketchAccumulator>().unwrap();
                (0..5)
                    .map(|k| hydra.query_key(&[format!("k{k}")], 0.5))
                    .collect()
            }
            SketchKernel::Hll(_) => {
                let hll = any.downcast_ref::<HllSketchAccumulator>().unwrap();
                hll.registers().iter().map(|&r| f64::from(r)).collect()
            }
            SketchKernel::WeightedFrequency(_) => {
                let frequency = any.downcast_ref::<WeightedFrequency>().unwrap();
                let mut out = frequency.counters().to_vec();
                out.extend(frequency.rows(4).iter().flatten().filter_map(|v| match v {
                    Value::Float64(x) => Some(*x),
                    _ => None,
                }));
                out
            }
            SketchKernel::UnivMon { .. } => [
                SketchStatistic::FrequencyL2,
                SketchStatistic::FrequencyEntropy,
            ]
            .iter()
            .map(|statistic| state.estimate(statistic).unwrap())
            .collect(),
        }
    }

    /// Readouts equal. A merged UnivMon is compared on its L2 estimate
    /// only: its merge breaks heap ties in a nondeterministic order
    /// (asap_sketchlib #266), so the heavy hitters, and the entropy read
    /// from them, differ between two merges of the same states.
    fn assert_same_readout(kernel: SketchKernel, a: Vec<f64>, b: Vec<f64>, what: &str) {
        if matches!(kernel, SketchKernel::UnivMon { .. }) && what.ends_with("merge") {
            assert_eq!(a[0], b[0], "{what}: {a:?} vs {b:?}");
        } else {
            assert_eq!(a, b, "{what}");
        }
    }

    /// Each persisted kernel's state survives bytes: re-encoding gives the
    /// same bytes, and estimates and merges read as before.
    #[test]
    fn sketch_states_round_trip_through_bytes() {
        for (kernel, a, b) in states() {
            let (kind, bytes_a) = encode_sketch(a.as_ref()).unwrap();
            assert_eq!(kind, kernel);
            let (_, bytes_b) = encode_sketch(b.as_ref()).unwrap();
            let a2 = decode_sketch(kernel, &bytes_a).unwrap();
            let b2 = decode_sketch(kernel, &bytes_b).unwrap();
            assert_eq!(encode_sketch(a2.as_ref()).unwrap().1, bytes_a, "{kernel:?}");
            assert_same_readout(
                kernel,
                readout(kernel, a.as_ref()),
                readout(kernel, a2.as_ref()),
                &format!("{kernel:?}"),
            );
            let merged = a.merge_with(b.as_ref()).unwrap();
            let merged2 = a2.merge_with(b2.as_ref()).unwrap();
            assert_same_readout(
                kernel,
                readout(kernel, merged.as_ref()),
                readout(kernel, merged2.as_ref()),
                &format!("{kernel:?} merge"),
            );
            if !matches!(kernel, SketchKernel::UnivMon { .. }) {
                assert_eq!(
                    encode_sketch(merged.as_ref()).unwrap().1,
                    encode_sketch(merged2.as_ref()).unwrap().1,
                    "{kernel:?} merge bytes"
                );
            }
        }
    }

    /// A Hydra-Count-Min grid shared by several groups is stored once per
    /// record, and its rebuilt grid answers every group's point counts as
    /// before.
    #[test]
    fn hydra_cms_grids_are_stored_once_and_rebuilt() {
        let mut grid = HydraCms::new(2, 16, 32, 3).unwrap();
        for (group, item) in [("g1", "a"), ("g1", "b"), ("g2", "a"), ("g2", "a")] {
            grid.count(group, item, 1).unwrap();
        }
        let grid = Arc::new(grid);
        let groups: Vec<HydraCmsGroup> = ["g1", "g2"]
            .iter()
            .map(|group| HydraCmsGroup {
                grid: grid.clone(),
                group: group.to_string(),
            })
            .collect();
        let mut grids = Grids::default();
        let stored: Vec<StoredValue> = groups
            .iter()
            .map(|group| encode_state(group, &mut grids).unwrap())
            .collect();
        assert_eq!(grids.bytes.len(), 1);
        let rebuilt = Arc::new(
            HydraCms::restore_from_disk(Hydra::from_asapv1(&grids.bytes[0]).unwrap()).unwrap(),
        );
        for value in stored {
            let StoredValue::HydraCmsGroup { grid: 0, group } = value else {
                panic!("not a Hydra-Count-Min group");
            };
            for item in ["a", "b", "c"] {
                assert_eq!(
                    rebuilt.point(&group, item).unwrap(),
                    grid.point(&group, item).unwrap()
                );
            }
        }
    }

    /// A torn last record is cut off, so a record appended after it is
    /// read back; a file of another plan is renamed and not loaded.
    #[test]
    fn torn_tails_are_cut_and_other_plans_are_not_loaded() {
        let dir = std::env::temp_dir().join(format!("asap-persist-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let empty: &[Batch] = &[];
        files.append(0, (0, 10), &[(1, empty)]).unwrap();
        drop(files);
        let path = dir.join("group-0.log");
        // A crash mid-record: a length announcing 100 bytes, 3 written.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[100, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(file);
        let files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        assert_eq!(files.read_group(&path, 0).unwrap().unwrap().len(), 1);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        files.append(0, (10, 20), &[(1, empty)]).unwrap();
        let records = files.read_group(&path, 0).unwrap().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| (r.start_ms, r.end_ms))
                .collect::<Vec<_>>(),
            [(0, 10), (10, 20)]
        );
        // A garbage length past the end is a torn record, not an allocation.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&u32::MAX.to_le_bytes()).unwrap();
        drop(file);
        assert_eq!(files.read_group(&path, 0).unwrap().unwrap().len(), 2);
        let two_records = std::fs::metadata(&path).unwrap().len();
        // A record that does not decode ends the file there: the records
        // after it are not loaded (they would read the per-file text
        // dictionary shifted) and are cut off.
        drop(files);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[3, 0, 0, 0, 0xc1, 0xc1, 0xc1]).unwrap();
        drop(file);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        files.append(0, (20, 30), &[(1, empty)]).unwrap();
        let records = files.read_group(&path, 0).unwrap().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| (r.start_ms, r.end_ms))
                .collect::<Vec<_>>(),
            [(0, 10), (10, 20)]
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), two_records);
        // Another plan's file is set aside.
        let other = ViewFiles::open(&dir, 8, Duration::ZERO).unwrap();
        assert!(other.read_group(&path, 0).unwrap().is_none());
        assert!(dir.join("group-0.log.stale").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
