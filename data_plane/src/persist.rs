//! Materialized views on disk: one append-only file per refresh group,
//! `group-<index>.log`, one record per refreshed window holding its
//! `(start, end]` and the rows of each view the window was appended to,
//! synced to disk (one `fdatasync`) before the next window is refreshed.
//! At startup the records still within each view's retention are loaded
//! back into the view store; open windows and raw samples are not
//! persisted.
//!
//! A file starts with a header naming the plan (a hash of the plan file)
//! and the group; a file written for another plan or group is renamed to
//! `*.stale` and never loaded. A record is a little-endian `u32` length
//! and a MessagePack payload; a torn or oversized record at the end (a
//! crash while writing) is cut off at load, so later records stay aligned.
//!
//! A row's plain values are stored as the executor's `Value`s, an exact
//! state (sum, count, min, max, rate, increase) by its serde form, and a
//! sketch state by `asap_sketchlib`'s ASAPv1 bytes with the kernel it
//! rebuilds: DDSketch, KLL, Count-Min, KMV and Hydra-KLL. A HyperLogLog,
//! UnivMon, weighted-frequency (Count-Min with heap) or Hydra-Count-Min
//! state has no public way to rebuild its kernel, so a view holding one is
//! not persisted (counted in `asap_view_windows_not_persisted_total`).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::summary_kernels::exact::ExactAccumulator;
use asap_executor::summary_kernels::{
    CountMinSketchAccumulator, DDSketchAccumulator, DatasketchesKLLAccumulator,
    HydraKllSketchAccumulator, IncreaseAccumulator, KmvAccumulator,
};
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::AggregateCore;
use asap_sketchlib::{CountMinF64, DdSketch, Hydra, Kll, Kmv};
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

/// The kernels whose sketch state is stored as ASAPv1 bytes.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SketchKernel {
    DDSketch,
    Kll,
    CountMin,
    Kmv,
    HydraKll,
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
    views: Vec<(PhysicalASAPNodeId, Vec<Vec<StoredValue>>)>,
}

/// One view's stored rows of one window: `(start, end, rows)`.
type StoredWindow = (i64, i64, Vec<Vec<StoredValue>>);

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
        };
        for (view, batches) in views {
            let mark = texts.new.len();
            match encode_rows(batches, &mut texts) {
                Some(rows) => stored.push((*view, rows)),
                None => {
                    texts.forget_after(mark);
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
                for (view, rows) in record.views {
                    by_view
                        .entry(view)
                        .or_default()
                        .push((record.start_ms, record.end_ms, rows));
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
                let retention_ms = i64::try_from(view.retention_ms()).unwrap_or(i64::MAX);
                let oldest_kept_end = newest_end.saturating_sub(retention_ms);
                for (start, end, rows) in own {
                    if end <= oldest_kept_end && end != newest_end {
                        continue;
                    }
                    let Some(batch) = decode_rows(schema, rows, &texts) else {
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
        let (frames, good_len) = read_frames(file)?;
        let mut frames = frames.into_iter();
        let header: Option<Header> = frames.next().and_then(|f| rmp_serde::from_slice(&f).ok());
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
        let file = OpenOptions::new().write(true).open(path)?;
        if file.metadata()?.len() != good_len {
            tracing::warn!(path = %path.display(), good_len, "cutting off a torn record");
            file.set_len(good_len)?;
            file.sync_data()?;
        }
        Ok(Some(
            frames
                .filter_map(|frame| rmp_serde::from_slice(&frame).ok())
                .collect(),
        ))
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

/// Every whole record of `file`'s, and the length up to the last one.
fn read_frames(file: File) -> std::io::Result<(Vec<Vec<u8>>, u64)> {
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
    Ok((frames, good_len))
}

/// `state` as stored bytes, when its kernel can be rebuilt from them.
fn encode_state(state: &dyn AggregateCore) -> Option<StoredValue> {
    let any = state.as_any();
    let sketch = |kernel, bytes: Result<Vec<u8>, _>| {
        Some(StoredValue::Sketch {
            kernel,
            asapv1: bytes.ok()?,
        })
    };
    if let Some(exact) = any.downcast_ref::<ExactAccumulator>() {
        Some(StoredValue::Exact(exact.clone()))
    } else if let Some(increase) = any.downcast_ref::<IncreaseAccumulator>() {
        Some(StoredValue::Increase(increase.clone()))
    } else if let Some(dd) = any.downcast_ref::<DDSketchAccumulator>() {
        sketch(SketchKernel::DDSketch, dd.inner.to_asapv1())
    } else if let Some(kll) = any.downcast_ref::<DatasketchesKLLAccumulator>() {
        sketch(SketchKernel::Kll, kll.inner.to_asapv1())
    } else if let Some(cms) = any.downcast_ref::<CountMinSketchAccumulator>() {
        sketch(SketchKernel::CountMin, cms.inner.to_asapv1())
    } else if let Some(kmv) = any.downcast_ref::<KmvAccumulator>() {
        sketch(SketchKernel::Kmv, kmv.inner.to_asapv1())
    } else if let Some(hydra) = any.downcast_ref::<HydraKllSketchAccumulator>() {
        sketch(SketchKernel::HydraKll, hydra.inner.to_asapv1())
    } else {
        None
    }
}

/// The kernel state `kernel` rebuilds from `asapv1`.
pub fn decode_sketch(kernel: SketchKernel, asapv1: &[u8]) -> Option<Arc<dyn AggregateCore>> {
    Some(match kernel {
        SketchKernel::DDSketch => Arc::new(DDSketchAccumulator {
            inner: DdSketch::from_asapv1(asapv1).ok()?,
        }),
        SketchKernel::Kll => Arc::new(DatasketchesKLLAccumulator {
            inner: Kll::from_asapv1(asapv1).ok()?,
        }),
        SketchKernel::CountMin => Arc::new(CountMinSketchAccumulator {
            inner: CountMinF64::from_asapv1(asapv1).ok()?,
        }),
        SketchKernel::Kmv => Arc::new(KmvAccumulator {
            inner: Kmv::from_asapv1(asapv1).ok()?,
        }),
        SketchKernel::HydraKll => Arc::new(HydraKllSketchAccumulator {
            inner: Hydra::from_asapv1(asapv1).ok()?,
        }),
    })
}

/// `state` as ASAPv1 bytes with its kernel, if it is one of the sketch
/// kernels persisted.
pub fn encode_sketch(state: &dyn AggregateCore) -> Option<(SketchKernel, Vec<u8>)> {
    match encode_state(state)? {
        StoredValue::Sketch { kernel, asapv1 } => Some((kernel, asapv1)),
        _ => None,
    }
}

/// Texts being encoded into one record: the file's dictionary, and the
/// texts this record adds to it.
struct Texts<'a> {
    dictionary: &'a Dictionary,
    new: Vec<Arc<str>>,
    new_index: std::collections::HashMap<Arc<str>, u32>,
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
/// holds a state with no byte encoding.
fn encode_rows(batches: &[Batch], texts: &mut Texts) -> Option<Vec<Vec<StoredValue>>> {
    batches
        .iter()
        .flat_map(Batch::to_rows)
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Summary { state, .. } => encode_state(state.as_ref()),
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

    /// Two states of each persisted sketch kernel, built from different
    /// samples.
    type StatePair = (SketchKernel, Box<dyn AggregateCore>, Box<dyn AggregateCore>);

    fn states() -> Vec<StatePair> {
        let mut out: Vec<StatePair> = Vec::new();
        let (mut a, mut b) = (
            DDSketchAccumulator::new(0.01),
            DDSketchAccumulator::new(0.01),
        );
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
        for n in 0..500 {
            let v = (n * 37 % 1000) as f64 + 0.5;
            let (first, key) = (n % 2 == 0, [format!("k{}", n % 5)]);
            if first {
                a.inner.add(&v);
                c.update(v);
                e.update(&key[0], 1.0 + v);
                g.insert_sample(v);
                i.update(&key, v);
            } else {
                b.inner.add(&v);
                d.update(v);
                f.update(&key[0], 1.0 + v);
                h.insert_sample(v);
                j.update(&key, v);
            }
        }
        out.push((SketchKernel::DDSketch, Box::new(a), Box::new(b)));
        out.push((SketchKernel::Kll, Box::new(c), Box::new(d)));
        out.push((SketchKernel::CountMin, Box::new(e), Box::new(f)));
        out.push((SketchKernel::Kmv, Box::new(g), Box::new(h)));
        out.push((SketchKernel::HydraKll, Box::new(i), Box::new(j)));
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
            assert_eq!(
                readout(kernel, a.as_ref()),
                readout(kernel, a2.as_ref()),
                "{kernel:?}"
            );
            let merged = a.merge_with(b.as_ref()).unwrap();
            let merged2 = a2.merge_with(b2.as_ref()).unwrap();
            assert_eq!(
                readout(kernel, merged.as_ref()),
                readout(kernel, merged2.as_ref()),
                "{kernel:?} merge"
            );
            assert_eq!(
                encode_sketch(merged.as_ref()).unwrap().1,
                encode_sketch(merged2.as_ref()).unwrap().1,
                "{kernel:?} merge bytes"
            );
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
        // Another plan's file is set aside.
        let other = ViewFiles::open(&dir, 8, Duration::ZERO).unwrap();
        assert!(other.read_group(&path, 0).unwrap().is_none());
        assert!(dir.join("group-0.log.stale").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
