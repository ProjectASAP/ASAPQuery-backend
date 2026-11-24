//! Materialized views on disk (docs/design/tiered-view-storage.md): per
//! refresh group and retention of its views, a directory
//! `group-<index>/retention-<ms>/` of time segments (`<first window end
//! ms>.seg`, see [`crate::segment`]), so each view's windows are deleted at
//! its own retention, not its group's longest; one record per
//! refreshed window holding its `(start, end]` and one columnar fragment
//! per view appended. The files are both the views' recovery and their cold
//! tier: a window evicted from memory is read back from its fragment.
//! Segments are synced every sync interval, roll every `segment_ms` of
//! windows, and are deleted once every window in them is past their views'
//! retention.
//! Once a newer sealed segment exists, a segment's pages are advised cold
//! ([`crate::segment::Segment::mark_cold`]): queries read a view's newest
//! windows on every evaluation, an older window only once more, and
//! without memory pressure the kernel would keep every page read while
//! the segment was young on its active list.
//! At startup the segments are indexed from their record heads, without
//! decoding any fragment.
//!
//! A segment starts with a header naming the plan (a hash of the plan
//! file), the group and the format version; a segment of another plan,
//! group or version, and a version 1 file (`group-<index>.log`), is renamed
//! `*.stale` and never read. A torn record at a segment's end (a crash
//! while writing), or the first record that does not decode and
//! everything after it, is cut off at startup, before the segment is
//! mapped: a later record's text keys would index the segment's texts
//! shifted.
//!
//! A state is stored by its existing form: an exact state by its serde
//! form, and a sketch state by `asap_sketchlib`'s ASAPv1 bytes with the
//! kernel it rebuilds, through each kernel's `sketch()` and
//! `restore_from_disk`. A Hydra-Count-Min grid, shared by every group of a
//! build, is stored once per record. A UnivMon whose heaps mix key types
//! has no ASAPv1 encoding, so a view holding one is not persisted (counted
//! in `asap_view_windows_not_persisted_total`) and stays in memory.

use std::collections::{BTreeMap, BTreeSet, HashMap};
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

use crate::segment::{encode_fragment, RecordHead, Segment, SegmentHeader, SegmentTexts};
pub use crate::segment::{ViewRead, WindowLocation, FORMAT_VERSION};

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

/// One stored state (or, in version 1 files, value) of a view row.
#[derive(Serialize, Deserialize)]
pub(crate) enum StoredValue {
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

/// What persisting one window wrote.
#[derive(Debug, Default, Clone)]
pub struct Written {
    /// Bytes added to the files.
    pub bytes: u64,
    pub sync_ns: u64,
    /// Views whose window could not be encoded.
    pub not_persisted: u64,
    /// Where each persisted view's fragment of the window is, for reading
    /// it back cold.
    pub locations: Vec<(PhysicalASAPNodeId, WindowLocation)>,
    /// Segments marked cold by this append ([`RetentionFiles::cool`]).
    pub segments_marked_cold: u64,
}

/// The windows indexed at startup, by view: each window's range and
/// location, oldest first.
pub type ViewIndex = BTreeMap<PhysicalASAPNodeId, Vec<((i64, i64), WindowLocation)>>;

/// The segment being appended to, with its texts by text.
struct ActiveSegment {
    segment: Arc<Segment>,
    texts: HashMap<Arc<str>, u32>,
    /// The start of its first window: it rolls once a window starts
    /// `segment_ms` or more after it.
    first_start_ms: i64,
}

/// The segments of one refresh group's views of one retention.
#[derive(Default)]
struct RetentionFiles {
    active: Option<ActiveSegment>,
    /// Sealed segments, oldest first.
    sealed: Vec<Arc<Segment>>,
}

impl RetentionFiles {
    /// Seals the active segment.
    fn seal_active(&mut self) -> std::io::Result<()> {
        if let Some(active) = self.active.take() {
            active.segment.seal()?;
            self.sealed.push(active.segment);
        }
        Ok(())
    }

    /// Marks cold every sealed segment but the newest: queries read a
    /// view's newest windows on every evaluation (the active segment and
    /// the newest sealed one), while an older window is read only once
    /// more, when it leaves a range's far edge. Returns how many it marked.
    fn cool(&self) -> u64 {
        let behind_newest = self.sealed.len().saturating_sub(1);
        self.sealed[..behind_newest]
            .iter()
            .filter(|segment| segment.mark_cold())
            .count() as u64
    }
}

/// The view files of some refresh groups in one directory, for one plan.
pub struct ViewFiles {
    dir: PathBuf,
    plan_hash: u64,
    segment_ms: i64,
    /// Each view's retention: a refresh group's views of one retention
    /// share segments, so each view's windows are deleted at its own
    /// retention. A view not listed is kept until its group's files go.
    view_retention_ms: BTreeMap<PhysicalASAPNodeId, i64>,
    /// The segments by refresh group and retention.
    files: BTreeMap<(usize, i64), RetentionFiles>,
    /// Files are synced at most this often; a window's record is durable
    /// within this long after it is written.
    sync_interval: Duration,
    last_sync: Instant,
    /// The (group, retention) files written since the last sync.
    unsynced: BTreeSet<(usize, i64)>,
}

/// A stable 64-bit FNV-1a hash of `bytes`.
pub fn plan_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// The default `segment_ms`: 15 minutes.
pub const DEFAULT_SEGMENT_MS: i64 = 900_000;

/// Renames `path` to `<path>.stale`, so it is never read again.
fn set_aside(path: &Path, why: &str) -> std::io::Result<()> {
    let mut stale = path.as_os_str().to_owned();
    stale.push(".stale");
    tracing::warn!(path = %path.display(), why, "view file set aside, not loaded");
    std::fs::rename(path, stale)
}

impl ViewFiles {
    /// The group files in `dir` (created if missing) of the plan whose
    /// file hashes to `plan_hash`.
    pub fn open(dir: &Path, plan_hash: u64, sync_interval: Duration) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            plan_hash,
            segment_ms: DEFAULT_SEGMENT_MS,
            view_retention_ms: BTreeMap::new(),
            files: BTreeMap::new(),
            sync_interval,
            last_sync: Instant::now(),
            unsynced: BTreeSet::new(),
        })
    }

    /// Rolls each group to a new segment once a window starts `segment_ms`
    /// or more after its active segment's first window.
    pub fn with_segment_ms(mut self, segment_ms: i64) -> Self {
        self.segment_ms = segment_ms.max(1);
        self
    }

    /// Writes and deletes each view's windows by its retention (in ms,
    /// past the newest window).
    pub fn with_view_retention_ms(
        mut self,
        view_retention_ms: BTreeMap<PhysicalASAPNodeId, i64>,
    ) -> Self {
        self.view_retention_ms = view_retention_ms;
        self
    }

    /// The retention `view`'s windows are written and deleted by.
    fn retention_of(&self, view: PhysicalASAPNodeId) -> i64 {
        self.view_retention_ms
            .get(&view)
            .copied()
            .unwrap_or(i64::MAX)
    }

    /// Splits the files into `shards` sets, refresh group `g` going to set
    /// `g % shards`: one writer per group, so records stay in window order.
    pub fn split(self, shards: usize) -> Vec<ViewFiles> {
        let shards = shards.max(1);
        let mut out: Vec<ViewFiles> = (0..shards)
            .map(|_| ViewFiles {
                dir: self.dir.clone(),
                plan_hash: self.plan_hash,
                segment_ms: self.segment_ms,
                view_retention_ms: self.view_retention_ms.clone(),
                files: BTreeMap::new(),
                sync_interval: self.sync_interval,
                last_sync: Instant::now(),
                unsynced: BTreeSet::new(),
            })
            .collect();
        for ((group, retention_ms), files) in self.files {
            out[group % shards]
                .files
                .insert((group, retention_ms), files);
        }
        out
    }

    /// How often the files are synced.
    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// Bytes of the segments held, over every group.
    pub fn bytes(&self) -> u64 {
        self.files
            .values()
            .flat_map(|g| g.sealed.iter().chain(g.active.iter().map(|a| &a.segment)))
            .map(|segment| segment.len())
            .sum()
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
        for key in std::mem::take(&mut self.unsynced) {
            if let Some(active) = self.files.get(&key).and_then(|g| g.active.as_ref()) {
                active.segment.sync()?;
            }
        }
        self.last_sync = Instant::now();
        Ok(started.elapsed().as_nanos() as u64)
    }

    fn group_dir(&self, group: usize) -> PathBuf {
        self.dir.join(format!("group-{group}"))
    }

    /// The directory of `group`'s segments holding views of `retention_ms`.
    fn retention_dir(&self, group: usize, retention_ms: i64) -> PathBuf {
        self.group_dir(group)
            .join(format!("retention-{retention_ms}"))
    }

    /// Seals `group`'s active segments now; its next window starts new
    /// ones.
    pub fn seal(&mut self, group: usize) -> std::io::Result<()> {
        for (&key, files) in self.files.range_mut((group, i64::MIN)..=(group, i64::MAX)) {
            files.seal_active()?;
            files.cool();
            self.unsynced.remove(&key);
        }
        Ok(())
    }

    /// Deletes `group`'s sealed segments whose every record is past its
    /// views' retention of `newest_end_ms` (ends at or before
    /// `newest_end_ms - retention`); returns how many. A reader still
    /// holding one keeps reading it (the file stays open until it is done).
    pub fn delete_expired(&mut self, group: usize, newest_end_ms: i64) -> std::io::Result<usize> {
        let mut deleted = 0;
        for (&(_, retention_ms), files) in
            self.files.range_mut((group, i64::MIN)..=(group, i64::MAX))
        {
            let oldest_kept_end_ms = newest_end_ms.saturating_sub(retention_ms);
            let mut kept = Vec::with_capacity(files.sealed.len());
            let mut failed = None;
            for segment in files.sealed.drain(..) {
                if failed.is_some() || segment.max_end_ms() > oldest_kept_end_ms {
                    kept.push(segment);
                    continue;
                }
                match std::fs::remove_file(segment.path()) {
                    Ok(()) => deleted += 1,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => deleted += 1,
                    Err(error) => {
                        kept.push(segment);
                        failed = Some(error);
                    }
                }
            }
            files.sealed = kept;
            if let Some(error) = failed {
                return Err(error);
            }
        }
        Ok(deleted)
    }

    /// Appends the rows each of `views` got for `(start, end]` as one record
    /// per retention of `group`'s views, each to the active segment of that
    /// retention (rolling to a new one when due), synced with the other
    /// files once the sync interval has passed. A view holding a state with
    /// no byte encoding is left out of the record and counted.
    pub fn append(
        &mut self,
        group: usize,
        range_ms: (i64, i64),
        views: &[(PhysicalASAPNodeId, &[Batch])],
    ) -> std::io::Result<Written> {
        let mut by_retention: BTreeMap<i64, Vec<(PhysicalASAPNodeId, &[Batch])>> = BTreeMap::new();
        for &(view, batches) in views {
            by_retention
                .entry(self.retention_of(view))
                .or_default()
                .push((view, batches));
        }
        let mut written = Written::default();
        for (retention_ms, views) in by_retention {
            let part = self.append_record(group, retention_ms, range_ms, &views)?;
            written.bytes += part.bytes;
            written.not_persisted += part.not_persisted;
            written.locations.extend(part.locations);
            written.segments_marked_cold += part.segments_marked_cold;
        }
        written.sync_ns = self.sync_if_due()?;
        Ok(written)
    }

    /// Appends one record of `group`'s views of `retention_ms`.
    fn append_record(
        &mut self,
        group: usize,
        retention_ms: i64,
        (start_ms, end_ms): (i64, i64),
        views: &[(PhysicalASAPNodeId, &[Batch])],
    ) -> std::io::Result<Written> {
        let mut written = Written::default();
        let segment_ms = self.segment_ms;
        let header = SegmentHeader {
            plan_hash: self.plan_hash,
            group,
            version: FORMAT_VERSION,
        };
        let path = self
            .retention_dir(group, retention_ms)
            .join(format!("{end_ms}.seg"));
        let files = self.files.entry((group, retention_ms)).or_default();
        if files
            .active
            .as_ref()
            .is_some_and(|active| start_ms >= active.first_start_ms.saturating_add(segment_ms))
        {
            files.seal_active()?;
            written.segments_marked_cold = files.cool();
        }
        if files.active.is_none() {
            files.active = Some(ActiveSegment {
                segment: Segment::create(&path, &header)?,
                texts: HashMap::new(),
                first_start_ms: start_ms,
            });
            written.bytes += files.active.as_ref().expect("set").segment.len();
        }
        let active = files.active.as_mut().expect("set");
        let mut texts = SegmentTexts {
            known: &active.texts,
            base: active.segment.text_count() as u32,
            new: Vec::new(),
            new_index: HashMap::new(),
        };
        let mut grids = Grids::default();
        let mut fragments: Vec<(PhysicalASAPNodeId, Vec<u8>)> = Vec::new();
        for (view, batches) in views {
            let (mark, grids_mark) = (texts.new.len(), grids.bytes.len());
            let encoded = batches.first().and_then(|first| {
                let rows = window_rows(batches)?;
                encode_fragment(first.schema(), &rows, &mut texts, &mut grids)
            });
            match encoded {
                Some(fragment) => fragments.push((*view, fragment)),
                None if batches.is_empty() => {}
                None => {
                    texts.forget_after(mark);
                    grids.forget_after(grids_mark);
                    written.not_persisted += 1;
                }
            }
        }
        if fragments.is_empty() {
            return Ok(written);
        }
        let new_texts = texts.new;
        let texts_part =
            rmp_serde::to_vec(&new_texts.iter().map(|t| t.as_ref()).collect::<Vec<&str>>())
                .map_err(std::io::Error::other)?;
        let grids_part = if grids.bytes.is_empty() {
            Vec::new()
        } else {
            rmp_serde::to_vec(
                &grids
                    .bytes
                    .iter()
                    .map(|b| serde_bytes::ByteBuf::from(b.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(std::io::Error::other)?
        };
        let mut parts: Vec<&[u8]> = vec![&texts_part, &grids_part];
        parts.extend(fragments.iter().map(|(_, fragment)| fragment.as_slice()));
        let base = active.segment.text_count() as u32;
        let (head, frame_bytes) =
            active
                .segment
                .append_record(&parts, new_texts.clone(), end_ms, |placed| RecordHead {
                    start_ms,
                    end_ms,
                    texts: placed[0],
                    grids: placed[1],
                    views: fragments
                        .iter()
                        .zip(&placed[2..])
                        .map(|((view, _), (offset, len))| (*view, *offset, *len))
                        .collect(),
                })?;
        for (i, text) in new_texts.into_iter().enumerate() {
            active.texts.insert(text, base + i as u32);
        }
        written.locations = head
            .views
            .iter()
            .map(|&(view, offset, len)| {
                (
                    view,
                    WindowLocation::new(active.segment.clone(), (offset, len), head.grids),
                )
            })
            .collect();
        self.unsynced.insert((group, retention_ms));
        written.bytes += frame_bytes;
        Ok(written)
    }

    /// The windows of `groups` (each refresh group's views with their
    /// output schemas) within each view's retention of its newest window,
    /// with their locations, oldest first, read from record heads only:
    /// no fragment is decoded. Every segment found is sealed; appends start
    /// a new one. Records each view's retention, by which its windows are
    /// written and deleted from now on. A segment directly in a group's
    /// directory (written before segments were split by retention) is
    /// deleted with the group's longest retention. A version 1 file, and a
    /// segment of another plan, group or version, is renamed `*.stale`; a
    /// torn record at a segment's end is cut off.
    pub fn index(
        &mut self,
        groups: &[Vec<(MaterializedView, SchemaRef)>],
    ) -> std::io::Result<ViewIndex> {
        let millis = |view: &MaterializedView| crate::ingestion::millis(view.retention_ms());
        for (view, _) in groups.iter().flatten() {
            self.view_retention_ms.insert(view.node_id, millis(view));
        }
        let mut index = ViewIndex::new();
        for (group, group_views) in groups.iter().enumerate() {
            let v1 = self.dir.join(format!("group-{group}.log"));
            if v1.exists() {
                set_aside(&v1, "a version 1 view file")?;
            }
            let longest_ms = group_views.iter().map(|(view, _)| millis(view)).max();
            let segments = |dir: &Path| -> Vec<(i64, PathBuf)> {
                std::fs::read_dir(dir)
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok().map(|e| e.path()))
                    .filter(|path| path.extension().is_some_and(|x| x == "seg"))
                    .filter_map(|path| {
                        let end = path.file_stem()?.to_str()?.parse::<i64>().ok()?;
                        Some((end, path))
                    })
                    .collect()
            };
            let dir = self.group_dir(group);
            // (retention, first window end, path), oldest first.
            let mut paths: Vec<(i64, i64, PathBuf)> = Vec::new();
            if let Some(longest_ms) = longest_ms {
                paths.extend(
                    segments(&dir)
                        .into_iter()
                        .map(|(end, path)| (longest_ms, end, path)),
                );
            }
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let retention_ms = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("retention-")?.parse::<i64>().ok());
                if let Some(retention_ms) = retention_ms {
                    paths.extend(
                        segments(&entry.path())
                            .into_iter()
                            .map(|(end, path)| (retention_ms, end, path)),
                    );
                }
            }
            paths.sort();
            let expected = SegmentHeader {
                plan_hash: self.plan_hash,
                group,
                version: FORMAT_VERSION,
            };
            let mut by_view = ViewIndex::new();
            for (retention_ms, _, path) in paths {
                let Some((segment, heads)) = Segment::open(&path, &expected)? else {
                    set_aside(&path, "a segment of another plan, group or version")?;
                    continue;
                };
                for head in heads {
                    for &(view, offset, len) in &head.views {
                        by_view.entry(view).or_default().push((
                            (head.start_ms, head.end_ms),
                            WindowLocation::new(segment.clone(), (offset, len), head.grids),
                        ));
                    }
                }
                self.files
                    .entry((group, retention_ms))
                    .or_default()
                    .sealed
                    .push(segment);
            }
            for (_, files) in self.files.range((group, i64::MIN)..=(group, i64::MAX)) {
                files.cool();
            }
            for (view, _) in group_views {
                let Some(mut windows) = by_view.remove(&view.node_id) else {
                    continue;
                };
                windows.sort_by_key(|(range, _)| *range);
                let Some(newest_end) = windows.last().map(|(range, _)| range.1) else {
                    continue;
                };
                let oldest_kept_end = crate::store::oldest_kept_end(view, newest_end);
                windows.retain(|((_, end), _)| *end > oldest_kept_end || *end == newest_end);
                index.insert(view.node_id, windows);
            }
        }
        Ok(index)
    }
}

/// The rows of a window's `batches`. A window split by series is kept as
/// one batch per part; its rows are written in the order one run over
/// every series emits them (by their plain columns' values), so a window's
/// fragment is the same whichever way its run was split. `None` when a
/// plain value has no order.
fn window_rows(batches: &[Batch]) -> Option<Vec<Vec<Value>>> {
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
    Some(rows)
}

/// The state `value` stores, with the record's rebuilt `grids`.
pub(crate) fn decode_stored_state(
    value: StoredValue,
    grids: &[Option<Arc<HydraCms>>],
) -> Option<Arc<dyn AggregateCore>> {
    Some(match value {
        StoredValue::Plain(_) | StoredValue::Text(_) => return None,
        StoredValue::Exact(state) => Arc::new(state),
        StoredValue::Increase(state) => Arc::new(state),
        StoredValue::Sketch { kernel, asapv1 } => decode_sketch(kernel, &asapv1)?,
        StoredValue::HydraCmsGroup { grid, group } => Arc::new(HydraCmsGroup {
            grid: grids.get(grid as usize)?.clone()?,
            group,
        }),
    })
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
pub(crate) struct Grids {
    pub(crate) bytes: Vec<Vec<u8>>,
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
    pub(crate) fn forget_after(&mut self, mark: usize) {
        self.bytes.truncate(mark);
        self.index.retain(|_, index| (*index as usize) < mark);
    }
}

/// `state` as stored bytes, when its kernel can be rebuilt from them; a
/// Hydra-Count-Min group's grid goes to `grids`.
pub(crate) fn encode_state(state: &dyn AggregateCore, grids: &mut Grids) -> Option<StoredValue> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use asap_executor::planner::summary_type::SketchStatistic;
    use asap_executor::values::Value;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

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

    // ── Tiered view storage: segments and columnar fragments ──────────
    // docs/design/tiered-view-storage.md, tests V2, V3, V9, V12, V13,
    // V18, V19. They fail until the segment format is implemented.

    use asap_executor::arrow::array::{DictionaryArray, Float64Array};
    use asap_executor::arrow::datatypes::UInt32Type;
    use asap_executor::planner::ir::schema::{
        DataType, Field, FieldDataType, Schema, PROMQL_SERIES_IDENTITY,
    };
    use asap_executor::planner::summary_type::{ExactKind, ExactParams, SketchAlgorithm};

    /// A fresh directory for one test's segments.
    fn segment_dir(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("asap-segments-{test}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// The view column family a state of `kernel` has, if a view column
    /// can hold it (Hydra-KLL and heapless weighted frequency cannot).
    fn family(kernel: SketchKernel) -> Option<FieldDataType> {
        use SketchAlgorithm as A;
        Some(FieldDataType::Sketch(match kernel {
            SketchKernel::DDSketch => A::DDSketch,
            SketchKernel::Kll => A::Kll,
            SketchKernel::CountMin => A::Cms,
            SketchKernel::Kmv => A::Kmv,
            SketchKernel::Hll(_) => A::Hll,
            SketchKernel::WeightedFrequency(FrequencyKind::CmsHeap) => A::CmsWithHeap,
            SketchKernel::WeightedFrequency(FrequencyKind::CountSketchHeap) => {
                A::CountSketchWithHeap
            }
            SketchKernel::UnivMon { .. } => A::UnivMon,
            _ => return None,
        }))
    }

    /// A view window's batch: a time column, a series identity, a label,
    /// a nullable value with one null, and a state column of `family`
    /// holding `states` (one row each).
    fn window_batch(
        end_ms: i64,
        family: FieldDataType,
        states: Vec<Arc<dyn AggregateCore>>,
    ) -> Batch {
        let schema = Arc::new(Schema::with_time_index(
            vec![
                Field::plain("ts", DataType::Timestamp, false),
                Field::plain(PROMQL_SERIES_IDENTITY, DataType::Utf8, false),
                Field::plain("job", DataType::Utf8, false),
                Field::plain("v", DataType::Float64, true),
                Field::new("state", family.clone(), false),
            ],
            0,
            vec![],
        ));
        let rows = states
            .into_iter()
            .enumerate()
            .map(|(row, state)| {
                vec![
                    Value::Timestamp(end_ms),
                    Value::Utf8(format!(r#"{{"__name__":"data","instance":"i{row}"}}"#).into()),
                    Value::Utf8(format!("job{}", row % 2).into()),
                    if row == 1 {
                        Value::Null
                    } else {
                        Value::Float64(row as f64 + 0.25)
                    },
                    Value::Summary {
                        family: family.clone(),
                        state,
                    },
                ]
            })
            .collect();
        Batch::try_new(schema, rows).unwrap()
    }

    /// A state's bytes, to compare two states: ASAPv1 for sketches, the
    /// serde form for exact states, the grid's bytes and the group for a
    /// Hydra-Count-Min group.
    fn state_bytes(state: &dyn AggregateCore) -> Vec<u8> {
        if let Some((_, bytes)) = encode_sketch(state) {
            return bytes;
        }
        let any = state.as_any();
        if let Some(exact) = any.downcast_ref::<ExactAccumulator>() {
            return rmp_serde::to_vec(exact).unwrap();
        }
        if let Some(group) = any.downcast_ref::<HydraCmsGroup>() {
            let mut bytes = group.grid.sketch().to_asapv1().unwrap();
            bytes.extend_from_slice(group.group.as_bytes());
            return bytes;
        }
        panic!("no byte form for this state");
    }

    /// Whether `read` holds the rows of `written`: plain values equal,
    /// states equal by their bytes.
    fn assert_same_rows(written: &Batch, read: &Batch, what: &str) {
        let (a, b) = (written.to_rows(), read.to_rows());
        assert_eq!(a.len(), b.len(), "{what}: row count");
        for (row, (x, y)) in a.iter().zip(&b).enumerate() {
            assert_eq!(x.len(), y.len(), "{what}: row {row} width");
            for (column, (x, y)) in x.iter().zip(y).enumerate() {
                match (x, y) {
                    (
                        Value::Summary {
                            family: fx,
                            state: sx,
                        },
                        Value::Summary {
                            family: fy,
                            state: sy,
                        },
                    ) => {
                        assert_eq!(fx, fy, "{what}: row {row} column {column} family");
                        // A DDSketch read back is shrunk (sealed), and a
                        // KLL read from its view is rebuilt as an
                        // accumulator (level 0 sorted), so their bytes may
                        // differ from the written state's; their answers
                        // are the same.
                        match encode_sketch(sx.as_ref()) {
                            Some((kernel @ (SketchKernel::DDSketch | SketchKernel::Kll), _)) => {
                                assert_same_readout(
                                    kernel,
                                    readout(kernel, sx.as_ref()),
                                    readout(kernel, sy.as_ref()),
                                    &format!("{what}: row {row} column {column} state"),
                                )
                            }
                            _ => assert_eq!(
                                state_bytes(sx.as_ref()),
                                state_bytes(sy.as_ref()),
                                "{what}: row {row} column {column} state"
                            ),
                        }
                    }
                    (x, y) => assert_eq!(
                        format!("{x:?}"),
                        format!("{y:?}"),
                        "{what}: row {row} column {column}"
                    ),
                }
            }
        }
    }

    /// The location `written` reports for `view`.
    fn location_of(written: &Written, view: PhysicalASAPNodeId) -> WindowLocation {
        written
            .locations
            .iter()
            .find(|(v, _)| *v == view)
            .map(|(_, location)| location.clone())
            .unwrap_or_else(|| panic!("no location for view {view}: segments are not implemented"))
    }

    /// One window per state family a view column can hold, plus exact
    /// Sum, Max and Counter (the state `rate` and `increase` read) and
    /// Hydra-Count-Min groups.
    fn every_family() -> Vec<(String, Batch)> {
        let mut out = Vec::new();
        for (kernel, a, b) in states() {
            let Some(family) = family(kernel) else {
                continue;
            };
            let (a, b): (Arc<dyn AggregateCore>, Arc<dyn AggregateCore>) =
                (Arc::from(a), Arc::from(b));
            out.push((
                format!("{kernel:?}"),
                window_batch(10, family, vec![a.clone(), b, a]),
            ));
        }
        for (kind, params) in [
            (ExactKind::Sum, ExactParams::Sum),
            (ExactKind::Max, ExactParams::Max),
            (ExactKind::Counter, ExactParams::Counter),
        ] {
            let family = FieldDataType::ExactAggregate(kind, params);
            let states: Vec<Arc<dyn AggregateCore>> = (0..3)
                .map(|row| {
                    let mut state = ExactAccumulator::new(family.clone(), false).unwrap();
                    for sample in 0..15 {
                        state.update(None, (row * 100 + sample) as f64 * 0.5, sample * 1000);
                    }
                    Arc::new(state) as Arc<dyn AggregateCore>
                })
                .collect();
            out.push((format!("{kind:?}"), window_batch(10, family, states)));
        }
        let mut grid = HydraCms::new(2, 16, 32, 3).unwrap();
        for (group, item) in [("g1", "a"), ("g1", "b"), ("g2", "a")] {
            grid.count(group, item, 1).unwrap();
        }
        let grid = Arc::new(grid);
        let groups: Vec<Arc<dyn AggregateCore>> = ["g1", "g2", "g3"]
            .iter()
            .map(|group| {
                Arc::new(HydraCmsGroup {
                    grid: grid.clone(),
                    group: group.to_string(),
                }) as Arc<dyn AggregateCore>
            })
            .collect();
        out.push((
            "HydraCms".into(),
            window_batch(10, FieldDataType::Sketch(SketchAlgorithm::HydraCms), groups),
        ));
        out
    }

    /// V2. A cold window decodes, from its columnar fragment, to the rows
    /// that were committed, read from the active segment (by `pread`) and
    /// again once the segment is sealed (mapped), and with `ViewRead::Pread`.
    #[test]
    fn every_family_reads_back_from_its_fragment() {
        let dir = segment_dir("families");
        for (what, batch) in every_family() {
            let _ = std::fs::remove_dir_all(&dir);
            let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
            let written = files
                .append(0, (0, 10), &[(1, std::slice::from_ref(&batch))])
                .unwrap();
            assert_eq!(written.not_persisted, 0, "{what}");
            let location = location_of(&written, 1);
            assert!(
                !location.is_sealed(),
                "{what}: the newest segment is active"
            );
            let active = location.read(batch.schema(), ViewRead::Mmap).unwrap();
            assert_same_rows(&batch, &active, &format!("{what}, active segment"));
            files.seal(0).unwrap();
            assert!(location.is_sealed(), "{what}");
            let mapped = location.read(batch.schema(), ViewRead::Mmap).unwrap();
            assert_same_rows(&batch, &mapped, &format!("{what}, mapped"));
            let pread = location.read(batch.schema(), ViewRead::Pread).unwrap();
            assert_same_rows(&batch, &pread, &format!("{what}, pread"));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A window with no rows (every series filtered out) reads back as an
    /// empty batch from the active and from a sealed segment; its empty
    /// fixed-width columns are aligned buffers too.
    #[test]
    fn an_empty_window_reads_back_empty() {
        let dir = segment_dir("empty");
        let (_, batch) = every_family().remove(0);
        let empty = window_batch(10, batch.schema().fields[4].dtype.clone(), Vec::new());
        assert_eq!(empty.num_rows(), 0);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let written = files
            .append(0, (0, 10), &[(1, std::slice::from_ref(&empty))])
            .unwrap();
        let location = location_of(&written, 1);
        for how in [ViewRead::Pread, ViewRead::Mmap] {
            assert_eq!(location.read(empty.schema(), how).unwrap().num_rows(), 0);
        }
        files.seal(0).unwrap();
        for how in [ViewRead::Pread, ViewRead::Mmap] {
            assert_eq!(location.read(empty.schema(), how).unwrap().num_rows(), 0);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// V3. A view's fragment decodes without the record's other views,
    /// and a damaged fragment is an error, not a panic or wrong rows.
    #[test]
    fn each_views_fragment_reads_alone() {
        let dir = segment_dir("fragments");
        let batches: Vec<Batch> = every_family().into_iter().take(4).map(|(_, b)| b).collect();
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let views: Vec<(PhysicalASAPNodeId, &[Batch])> = batches
            .iter()
            .enumerate()
            .map(|(i, batch)| (i as PhysicalASAPNodeId + 1, std::slice::from_ref(batch)))
            .collect();
        let written = files.append(0, (0, 10), &views).unwrap();
        files.seal(0).unwrap();
        let damaged = location_of(&written, 2);
        let mut file = OpenOptions::new()
            .write(true)
            .open(damaged.segment_path())
            .unwrap();
        file.seek(SeekFrom::Start(damaged.offset())).unwrap();
        file.write_all(&vec![0xff; damaged.len() as usize]).unwrap();
        drop(file);
        for (i, batch) in batches.iter().enumerate() {
            let view = i as PhysicalASAPNodeId + 1;
            let read = location_of(&written, view).read(batch.schema(), ViewRead::Mmap);
            if view == 2 {
                assert!(read.is_err(), "a damaged fragment must not decode");
            } else {
                assert_same_rows(batch, &read.unwrap(), &format!("view {view}"));
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// V18. Reading a sealed segment borrows the text keys and plain
    /// columns from the mapping, and every window of the segment shares one
    /// dictionary; `ViewRead::Pread` copies them and reads the same rows.
    #[test]
    fn mapped_reads_borrow_fixed_width_columns() {
        let dir = segment_dir("borrow");
        let (_, a, b) = states().remove(1);
        let (a, b): (Arc<dyn AggregateCore>, Arc<dyn AggregateCore>) = (Arc::from(a), Arc::from(b));
        let kll = FieldDataType::Sketch(SketchAlgorithm::Kll);
        let rows = |end| {
            window_batch(
                end,
                kll.clone(),
                (0..1000)
                    .map(|i| if i % 2 == 0 { a.clone() } else { b.clone() })
                    .collect(),
            )
        };
        let (first, second) = (rows(10), rows(20));
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let w1 = files
            .append(0, (0, 10), &[(1, std::slice::from_ref(&first))])
            .unwrap();
        let w2 = files
            .append(0, (10, 20), &[(1, std::slice::from_ref(&second))])
            .unwrap();
        files.seal(0).unwrap();
        let (l1, l2) = (location_of(&w1, 1), location_of(&w2, 1));
        let r1 = l1.read(first.schema(), ViewRead::Mmap).unwrap();
        let r2 = l2.read(second.schema(), ViewRead::Mmap).unwrap();
        assert_same_rows(&first, &r1, "mapped");
        let identity = |batch: &Batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .expect("a cold text column is a dictionary over the segment's texts")
                .clone()
        };
        let keys = identity(&r1).keys().values().as_ptr() as usize;
        assert!(l1.maps(keys), "text keys are copied, not borrowed");
        let values = r1
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            l1.maps(values.values().as_ptr() as usize),
            "values are copied, not borrowed"
        );
        // The KLL states are read in place (a mapped column, charged
        // nothing), not rebuilt as objects.
        let states = r1.state_column(4).expect("a state column");
        assert_eq!(states.state_memory_bytes(), 0);
        assert!(
            Arc::ptr_eq(identity(&r1).values(), identity(&r2).values()),
            "one dictionary per segment"
        );
        let copied = l1.read(first.schema(), ViewRead::Pread).unwrap();
        assert_same_rows(&first, &copied, "pread");
        assert!(!l1.maps(identity(&copied).keys().values().as_ptr() as usize));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// V19. A segment unlinked while a location still holds it stays
    /// readable (the mapping keeps its pages); no read can touch past the
    /// end of a mapped file.
    #[test]
    fn an_unlinked_sealed_segment_stays_readable() {
        let dir = segment_dir("unlinked");
        let (_, batch) = every_family().remove(0);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let written = files
            .append(0, (0, 10), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        files.seal(0).unwrap();
        let location = location_of(&written, 1);
        let before = location.read(batch.schema(), ViewRead::Mmap).unwrap();
        std::fs::remove_file(location.segment_path()).unwrap();
        let after = location.read(batch.schema(), ViewRead::Mmap).unwrap();
        assert_same_rows(&before, &after, "after unlink");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The segment files under `dir`, in every refresh group and
    /// retention directory.
    fn segment_files(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|x| x == "seg") {
                    out.push(path);
                }
            }
        }
        out.sort();
        out
    }

    /// V9 (unit). Segments roll every `segment_ms` of windows, and only
    /// sealed segments whose every record is past the view's retention of
    /// the newest window are deleted.
    #[test]
    fn segments_roll_and_expire_whole() {
        let dir = segment_dir("roll");
        let (_, batch) = every_family().remove(0);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .with_segment_ms(20)
            .with_view_retention_ms(BTreeMap::from([(1, 20)]));
        for end in (10..=60).step_by(10) {
            files
                .append(0, (end - 10, end), &[(1, std::slice::from_ref(&batch))])
                .unwrap();
        }
        assert_eq!(segment_files(&dir).len(), 3, "(0, 20], (20, 40], (40, 60]");
        assert_eq!(files.delete_expired(0, 60).unwrap(), 2);
        assert_eq!(segment_files(&dir).len(), 1);
        // The active segment is never deleted.
        assert_eq!(files.delete_expired(0, 1_000).unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Each view's windows are deleted at its own retention: a refresh
    /// group's views of different retentions are written to different
    /// segments, so a short-retention view's bytes leave the disk once past
    /// its retention while a long-retention view of the same group keeps
    /// its windows (and stays readable).
    #[test]
    fn each_view_expires_at_its_own_retention() {
        let dir = segment_dir("per-view-retention");
        let (_, batch) = every_family().remove(0);
        let (short, long) = (1, 2);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .with_segment_ms(20)
            .with_view_retention_ms(BTreeMap::from([(short, 20), (long, 1_000)]));
        let mut oldest = None;
        for end in (10..=80).step_by(10) {
            let written = files
                .append(
                    0,
                    (end - 10, end),
                    &[
                        (short, std::slice::from_ref(&batch)),
                        (long, std::slice::from_ref(&batch)),
                    ],
                )
                .unwrap();
            oldest.get_or_insert((location_of(&written, short), location_of(&written, long)));
        }
        let (oldest_short, oldest_long) = oldest.unwrap();
        assert_ne!(
            oldest_short.segment_path(),
            oldest_long.segment_path(),
            "views of different retentions share no segment"
        );
        let bytes_before = files.bytes();
        // Newest window ends at 80: the short view keeps windows ending
        // after 60, so its segments (0, 20] and (20, 40] go; (40, 60] ends
        // at the cutoff and goes too. The long view keeps everything.
        assert_eq!(files.delete_expired(0, 80).unwrap(), 3);
        assert!(!oldest_short.segment_path().exists());
        assert!(oldest_long.segment_path().exists());
        assert!(files.bytes() < bytes_before);
        let read = oldest_long.read(batch.schema(), ViewRead::Mmap).unwrap();
        assert_same_rows(&batch, &read, "the long-retention view's oldest window");
        let left: Vec<PathBuf> = segment_files(&dir);
        assert_eq!(left.len(), 5, "short: (60, 80]; long: four segments");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Once a newer sealed segment exists, older sealed segments are marked
    /// cold (their pages advised cold, once each): only the newest sealed
    /// segment and the active one stay hot. A cold segment reads back the
    /// same rows.
    #[test]
    fn segments_behind_the_newest_sealed_one_are_marked_cold() {
        let dir = segment_dir("cold");
        let (_, batch) = every_family().remove(0);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .with_segment_ms(20);
        let mut locations = Vec::new();
        let mut marked_cold = 0;
        for end in (10..=80).step_by(10) {
            let written = files
                .append(0, (end - 10, end), &[(1, std::slice::from_ref(&batch))])
                .unwrap();
            marked_cold += written.segments_marked_cold;
            locations.push(location_of(&written, 1));
            if end == 20 {
                // Map the first segment before it turns cold: the advice
                // applies to a live mapping.
                files.seal(0).unwrap();
                locations[0].read(batch.schema(), ViewRead::Mmap).unwrap();
            }
        }
        // Segments (0, 20], (20, 40], (40, 60] sealed; (60, 80] active.
        let cold: Vec<bool> = locations.iter().map(WindowLocation::is_cold).collect();
        assert_eq!(
            cold,
            [true, true, true, true, false, false, false, false],
            "only (0, 20] and (20, 40] are behind the newest sealed segment"
        );
        assert_eq!(marked_cold, 2, "each segment is marked once");
        files.seal(0).unwrap();
        assert!(locations[4].is_cold(), "sealing (60, 80] cools (40, 60]");
        assert!(!locations[6].is_cold());
        let read = locations[0].read(batch.schema(), ViewRead::Mmap).unwrap();
        assert_same_rows(&batch, &read, "a cold segment");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// V12 (unit). A torn record at the end of the active segment is cut
    /// off before indexing (so before any mapping), and later appends stay
    /// aligned; indexing decodes no fragment.
    #[test]
    fn a_torn_segment_tail_is_cut_and_appends_continue() {
        let dir = segment_dir("torn");
        let (_, batch) = every_family().remove(0);
        let view: MaterializedView = serde_json::from_value(serde_json::json!({
            "node_id": 1,
            "refresh": "at_ingestion",
            "coverage": {"definition_id": "v", "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap();
        let groups = vec![vec![(view, batch.schema().clone())]];
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let written = files
            .append(0, (0, 10), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        files
            .append(0, (10, 20), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        let path = location_of(&written, 1).segment_path().to_path_buf();
        drop(files);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[100, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(file);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let index = files.index(&groups).unwrap();
        let ranges: Vec<(i64, i64)> = index[&1].iter().map(|(range, _)| *range).collect();
        assert_eq!(ranges, [(0, 10), (10, 20)]);
        files
            .append(0, (20, 30), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        let index = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .index(&groups)
            .unwrap();
        assert_eq!(index[&1].len(), 3);
        for (_, location) in &index[&1] {
            assert_same_rows(
                &batch,
                &location.read(batch.schema(), ViewRead::Mmap).unwrap(),
                "after the cut",
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A whole record that does not decode ends its segment there: the
    /// records after it are not indexed (their text keys would index the
    /// segment's texts shifted) and are cut off with it.
    #[test]
    fn an_undecodable_record_ends_its_segment() {
        use std::os::unix::fs::FileExt;
        let dir = segment_dir("undecodable");
        let (_, batch) = every_family().remove(0);
        let view: MaterializedView = serde_json::from_value(serde_json::json!({
            "node_id": 1,
            "refresh": "at_ingestion",
            "coverage": {"definition_id": "v", "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap();
        let groups = vec![vec![(view, batch.schema().clone())]];
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let written = files
            .append(0, (0, 10), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        let path = location_of(&written, 1).segment_path().to_path_buf();
        let one_record = std::fs::metadata(&path).unwrap().len();
        for end in [20, 30] {
            files
                .append(0, (end - 10, end), &[(1, std::slice::from_ref(&batch))])
                .unwrap();
        }
        drop(files);
        // The second record's frame stays whole, but its kind is unknown.
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&9u32.to_le_bytes(), one_record + 4)
            .unwrap();
        drop(file);
        let index = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .index(&groups)
            .unwrap();
        let ranges: Vec<(i64, i64)> = index[&1].iter().map(|(range, _)| *range).collect();
        assert_eq!(ranges, [(0, 10)]);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), one_record);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// V13 (unit). A version 1 file and a segment of another plan are
    /// renamed `*.stale` and not indexed.
    #[test]
    fn old_format_and_other_plan_files_are_set_aside() {
        let dir = segment_dir("stale");
        let (_, batch) = every_family().remove(0);
        let view: MaterializedView = serde_json::from_value(serde_json::json!({
            "node_id": 1,
            "refresh": "at_ingestion",
            "coverage": {"definition_id": "v", "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap();
        let groups = vec![vec![(view, batch.schema().clone())]];
        std::fs::create_dir_all(&dir).unwrap();
        // A version 1 file of the same plan: its header frame and one empty
        // record, in the version 1 layout (spelled out here, since the
        // writer no longer produces it).
        #[derive(Serialize)]
        struct V1Header {
            plan_hash: u64,
            group: usize,
        }
        #[derive(Serialize)]
        struct V1Record {
            start_ms: i64,
            end_ms: i64,
            new_texts: Vec<String>,
            hydra_cms_grids: Vec<serde_bytes::ByteBuf>,
            views: Vec<(PhysicalASAPNodeId, Vec<Vec<u8>>)>,
        }
        let framed = |payload: Vec<u8>| -> Vec<u8> {
            let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
            bytes.extend(payload);
            bytes
        };
        let v1 = dir.join("group-0.log");
        let mut bytes = framed(
            rmp_serde::to_vec(&V1Header {
                plan_hash: 7,
                group: 0,
            })
            .unwrap(),
        );
        bytes.extend(framed(
            rmp_serde::to_vec(&V1Record {
                start_ms: 0,
                end_ms: 10,
                new_texts: vec![],
                hydra_cms_grids: vec![],
                views: vec![],
            })
            .unwrap(),
        ));
        std::fs::write(&v1, bytes).unwrap();
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
        let index = files.index(&groups).unwrap();
        assert!(index.get(&1).is_none_or(Vec::is_empty));
        assert!(!v1.exists() && dir.join("group-0.log.stale").exists());
        // A segment of plan 7 is set aside by a data plane of plan 8.
        let written = files
            .append(0, (0, 10), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        let segment = location_of(&written, 1).segment_path().to_path_buf();
        drop(files);
        let index = ViewFiles::open(&dir, 8, Duration::ZERO)
            .unwrap()
            .index(&groups)
            .unwrap();
        assert!(index.get(&1).is_none_or(Vec::is_empty));
        assert!(!segment.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Segments written before the split by retention, directly
    /// in `group-<g>/`, are indexed and kept until the group's longest
    /// retention.
    #[test]
    fn old_layout_segments_load_and_expire_at_the_longest_retention() {
        let dir = segment_dir("old-layout");
        let (_, batch) = every_family().remove(0);
        let view = |id: u64, retention: u64| -> MaterializedView {
            serde_json::from_value(serde_json::json!({
                "node_id": id,
                "refresh": "at_ingestion",
                "coverage": {"definition_id": format!("v{id}"), "selection": []},
                "inputs": {"materialized_views": []},
                "retention_ms": retention,
            }))
            .unwrap()
        };
        let groups = vec![vec![
            (view(1, 20), batch.schema().clone()),
            (view(2, 1_000), batch.schema().clone()),
        ]];
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .with_segment_ms(20);
        for end in (10..=80).step_by(10) {
            files
                .append(
                    0,
                    (end - 10, end),
                    &[
                        (1, std::slice::from_ref(&batch)),
                        (2, std::slice::from_ref(&batch)),
                    ],
                )
                .unwrap();
        }
        drop(files);
        // Move every segment up into group-0/, the old layout.
        let group_dir = dir.join("group-0");
        for path in segment_files(&dir) {
            std::fs::rename(&path, group_dir.join(path.file_name().unwrap())).unwrap();
        }
        for entry in std::fs::read_dir(&group_dir).unwrap().flatten() {
            if entry.path().is_dir() {
                std::fs::remove_dir_all(entry.path()).unwrap();
            }
        }
        let old: Vec<PathBuf> = segment_files(&dir);
        assert_eq!(old.len(), 4);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .with_segment_ms(20);
        let index = files.index(&groups).unwrap();
        assert_eq!(
            index[&2].len(),
            8,
            "the long view's windows: {:?}",
            index.keys()
        );
        for (_, location) in &index[&2] {
            assert_same_rows(
                &batch,
                &location.read(batch.schema(), ViewRead::Mmap).unwrap(),
                "old layout",
            );
        }
        assert!(!index[&1].is_empty());
        // New windows go to the retention directories.
        for end in (90..=200).step_by(10) {
            files
                .append(
                    0,
                    (end - 10, end),
                    &[
                        (1, std::slice::from_ref(&batch)),
                        (2, std::slice::from_ref(&batch)),
                    ],
                )
                .unwrap();
        }
        files.delete_expired(0, 200).unwrap();
        assert!(
            old.iter().all(|path| path.exists()),
            "old segments live until 1,000 ms"
        );
        files.delete_expired(0, 1_200).unwrap();
        assert!(old.iter().all(|path| !path.exists()), "and go past it");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unlink failure keeps the rest of the sealed list, so
    /// a later call deletes what it could not.
    #[test]
    fn a_failed_unlink_keeps_the_sealed_list() {
        use std::os::unix::fs::PermissionsExt;
        let dir = segment_dir("unlink");
        let (_, batch) = every_family().remove(0);
        let mut files = ViewFiles::open(&dir, 7, Duration::ZERO)
            .unwrap()
            .with_segment_ms(20)
            .with_view_retention_ms(BTreeMap::from([(1, 20)]));
        for end in (10..=100).step_by(10) {
            files
                .append(0, (end - 10, end), &[(1, std::slice::from_ref(&batch))])
                .unwrap();
        }
        assert_eq!(segment_files(&dir).len(), 5);
        let retention_dir = dir.join("group-0").join("retention-20");
        std::fs::set_permissions(&retention_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        assert!(files.delete_expired(0, 100).is_err());
        std::fs::set_permissions(&retention_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            files.delete_expired(0, 100).unwrap(),
            4,
            "every expired segment, after the failure"
        );
        assert_eq!(segment_files(&dir).len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
