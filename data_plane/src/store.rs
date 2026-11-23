//! The data plane's time-indexed store, which the executor reads through
//! `asap_executor::sources::DataStore`:
//!
//! - **raw data**: every Remote Write sample of a metric some `Scan` of the
//!   plan reads, as a time series table `(ts, value,
//!   $promql_series_identity, labels...)`. A read
//!   returns the samples in the request's time range; the executor's own
//!   `TimeRange`s keep each window, an instant selector's latest sample per
//!   series, and drop Prometheus stale markers.
//! - **materialized views**: each view's rows, appended by the ingestion
//!   run that refreshed them with the `(start, end]` they cover, kept for
//!   the view's `retention_ms` ([`ViewStore`]). A read returns exactly the
//!   rows tiling the requested range and fails when one is missing.
//!
//! Raw samples are held columnar, by pane ([`RawData`]): a Remote Write
//! request's samples of a pane become one immutable chunk, a pane the
//! watermark has passed is compacted into one chunk, and eviction drops
//! whole panes. A series' identity text is computed once, when it is
//! interned, and each row holds an index into its chunk's series.
//!
//! The views have a second, query-time partition ([`QueryTimeResults`]):
//! each query-time `OneEvaluation` node's output at one evaluation time,
//! computed once and shared by the queries at that time.
//!
//! A run reads a [`RunStore`]: an immutable snapshot sharing both parts.
//! Each copy of a part has one writer (raw data: each ingest shard, for
//! its own series; views: the precompute committer), which changes it
//! (copy on write: clones share every chunk and batch); the copies are
//! published ([`PublishedStore`]; raw data by the ingest coordinator), so
//! a run never sees a change and never blocks one.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use asap_executor::arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray,
    UInt32Array,
};
use asap_executor::arrow::datatypes::UInt32Type;
use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::schema::{DataType, FieldDataType, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::runtime::{OutputStream, RunContext};
use asap_executor::sources::{DataStore, MaterializedViewRequest, RawDataRequest};
use asap_executor::values::{Batch, BatchColumn, SchemaRef};
use asap_executor::Error;
use futures::{stream, StreamExt};

use crate::ingestion::window_end;
use crate::series_ids::Series;

/// One raw sample: its series (interned: its id and full label set,
/// `__name__` included), time and value.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    pub labels: Arc<Series>,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// Raw samples of one metric, columnar and immutable: one Remote Write
/// request's samples of a pane, or a closed pane's samples compacted into
/// one chunk. Rows keep arrival order.
#[derive(Debug)]
struct Chunk {
    time: TimestampMillisecondArray,
    value: Float64Array,
    /// Each row's series, as an index into `series`.
    codes: UInt32Array,
    /// The chunk's distinct series, in order of their first row.
    series: Vec<Arc<Series>>,
    /// Each series' identity text, by code: the dictionary of the series
    /// identity column, built by the first read that needs it and shared by
    /// every later one.
    identities: std::sync::OnceLock<ArrayRef>,
    /// The oldest and newest sample time.
    time_bounds_ms: (i64, i64),
}

impl Chunk {
    /// `rows` (series, time, value), in order; `None` when there is none.
    fn new<'a>(rows: impl Iterator<Item = (&'a Arc<Series>, i64, f64)>) -> Option<Self> {
        let mut code_of: HashMap<u32, u32> = HashMap::new();
        let mut series: Vec<Arc<Series>> = Vec::new();
        let (mut times, mut values, mut codes) = (Vec::new(), Vec::new(), Vec::new());
        for (row_series, time, value) in rows {
            let code = *code_of.entry(row_series.id).or_insert_with(|| {
                series.push(row_series.clone());
                series.len() as u32 - 1
            });
            times.push(time);
            values.push(value);
            codes.push(code);
        }
        let time_bounds_ms = (*times.iter().min()?, *times.iter().max()?);
        Some(Self {
            time: TimestampMillisecondArray::from(times),
            value: Float64Array::from(values),
            codes: UInt32Array::from(codes),
            series,
            identities: std::sync::OnceLock::new(),
            time_bounds_ms,
        })
    }

    fn rows(&self) -> impl Iterator<Item = (&Arc<Series>, i64, f64)> {
        self.codes
            .values()
            .iter()
            .zip(self.time.values().iter())
            .zip(self.value.values().iter())
            .map(|((&code, &time), &value)| (&self.series[code as usize], time, value))
    }

    fn len(&self) -> usize {
        self.time.len()
    }

    /// Each series' identity text, by code.
    fn identities(&self) -> &ArrayRef {
        self.identities.get_or_init(|| {
            Arc::new(StringArray::from_iter_values(
                self.series.iter().map(|s| &*s.identity),
            ))
        })
    }

    fn bytes(&self) -> usize {
        self.time.get_array_memory_size()
            + self.value.get_array_memory_size()
            + self.codes.get_array_memory_size()
            + self
                .identities
                .get()
                .map_or(0, |identities| identities.get_array_memory_size())
            + self.series.len() * std::mem::size_of::<Arc<Series>>()
    }

    /// The rows in `(start, end]`; the chunk itself when all are.
    fn in_range(self: &Arc<Self>, (start, end): (i64, i64)) -> Option<Arc<Self>> {
        let (oldest, newest) = self.time_bounds_ms;
        if start < oldest && newest <= end {
            return Some(self.clone());
        }
        if newest <= start || end < oldest {
            return None;
        }
        Self::new(self.rows().filter(|(_, t, _)| start < *t && *t <= end)).map(Arc::new)
    }

    /// The rows of the series in `partition`, in order: the chunk itself
    /// when every series is, `None` when none is. One pass over the rows,
    /// with no lookup per row: each series' new code is found once.
    fn in_partition(self: &Arc<Self>, partition: SeriesPartition) -> Option<Arc<Self>> {
        const DROPPED: u32 = u32::MAX;
        let mut new_code = vec![DROPPED; self.series.len()];
        let mut series = Vec::new();
        for (code, one) in self.series.iter().enumerate() {
            if partition.holds(one.id) {
                new_code[code] = series.len() as u32;
                series.push(one.clone());
            }
        }
        if series.len() == self.series.len() {
            return Some(self.clone());
        }
        if series.is_empty() {
            return None;
        }
        let (mut times, mut values, mut codes) = (Vec::new(), Vec::new(), Vec::new());
        let rows = self
            .codes
            .values()
            .iter()
            .zip(self.time.values().iter())
            .zip(self.value.values().iter());
        for ((&code, &time), &value) in rows {
            let code = new_code[code as usize];
            if code != DROPPED {
                times.push(time);
                values.push(value);
                codes.push(code);
            }
        }
        let time_bounds_ms = (*times.iter().min()?, *times.iter().max()?);
        Some(Arc::new(Self {
            time: TimestampMillisecondArray::from(times),
            value: Float64Array::from(values),
            codes: UInt32Array::from(codes),
            series,
            identities: std::sync::OnceLock::new(),
            time_bounds_ms,
        }))
    }

    fn column(&self, kind: &ColumnKind) -> Result<ArrayRef, Error> {
        Ok(match kind {
            ColumnKind::Time => Arc::new(self.time.clone()),
            ColumnKind::Value => Arc::new(self.value.clone()),
            ColumnKind::SeriesIdentity => Arc::new(
                DictionaryArray::try_new(self.codes.clone(), self.identities().clone())
                    .map_err(|e| Error::Invalid(e.to_string()))?,
            ),
            ColumnKind::Label(name) => {
                // Series sharing a label value share its dictionary entry.
                let mut code_of_text: HashMap<&str, u32> = HashMap::new();
                let mut texts: Vec<&str> = Vec::new();
                let label_code: Vec<u32> = self
                    .series
                    .iter()
                    .map(|series| {
                        let text = series.get(name).map_or("", String::as_str);
                        *code_of_text.entry(text).or_insert_with(|| {
                            texts.push(text);
                            texts.len() as u32 - 1
                        })
                    })
                    .collect();
                let keys = UInt32Array::from_iter_values(
                    self.codes.values().iter().map(|&c| label_code[c as usize]),
                );
                Arc::new(
                    DictionaryArray::try_new(keys, Arc::new(StringArray::from(texts)))
                        .map_err(|e| Error::Invalid(e.to_string()))?,
                )
            }
        })
    }
}

/// The samples of one pane `(end - width, end]` of a metric, as chunks.
type Pane = Vec<Arc<Chunk>>;

/// One metric's samples.
#[derive(Debug, Clone, Default)]
struct MetricPanes {
    /// Panes by end.
    panes: BTreeMap<i64, Pane>,
    /// Samples at or before this are evicted: a read never returns them,
    /// though the oldest pane kept may still hold some.
    evicted_through_ms: Option<i64>,
}

/// The raw samples of each metric the plan scans, columnar, by pane: the
/// panes `(end - width, end]` with `end` on multiples of `pane_width_ms`.
/// Cloning shares every chunk.
#[derive(Debug, Clone)]
pub struct RawData {
    pane_width_ms: i64,
    by_metric: HashMap<String, MetricPanes>,
}

impl RawData {
    /// Keeps the samples of `metrics` from now on, in panes of
    /// `pane_width_ms`; samples of any other metric are not stored
    /// ([`Self::reads`]).
    pub fn for_metrics<'a>(
        metrics: impl IntoIterator<Item = &'a String>,
        pane_width_ms: i64,
    ) -> Self {
        Self {
            pane_width_ms,
            by_metric: metrics
                .into_iter()
                .map(|metric| (metric.clone(), MetricPanes::default()))
                .collect(),
        }
    }

    /// Whether some `Scan` reads `metric`.
    pub fn reads(&self, metric: &str) -> bool {
        self.by_metric.contains_key(metric)
    }

    /// Appends `samples`, in order, as one chunk per metric and pane;
    /// samples of a metric no `Scan` reads are not stored.
    pub fn append(&mut self, samples: &[RawSample]) {
        let mut by_pane: BTreeMap<(&str, i64), Vec<&RawSample>> = BTreeMap::new();
        for sample in samples {
            let pane_end = window_end(sample.timestamp_ms, self.pane_width_ms);
            by_pane
                .entry((&sample.labels.metric, pane_end))
                .or_default()
                .push(sample);
        }
        for ((metric, pane_end), rows) in by_pane {
            let Some(metric) = self.by_metric.get_mut(metric) else {
                continue;
            };
            let chunk = Chunk::new(rows.iter().map(|s| (&s.labels, s.timestamp_ms, s.value)));
            metric
                .panes
                .entry(pane_end)
                .or_default()
                .extend(chunk.map(Arc::new));
        }
    }

    /// Compacts each pane ending at or before `watermark_ms` into one chunk:
    /// no more samples are expected for it (a late one adds a chunk, which
    /// the next call compacts).
    pub fn seal_through(&mut self, watermark_ms: i64) {
        for metric in self.by_metric.values_mut() {
            for (_, pane) in metric.panes.range_mut(..=watermark_ms) {
                if pane.len() > 1 {
                    let compacted = Chunk::new(pane.iter().flat_map(|chunk| chunk.rows()));
                    *pane = compacted.map(Arc::new).into_iter().collect();
                }
            }
        }
    }

    /// Evicts `metric`'s samples at or before `oldest_kept_ms`: no read
    /// returns them, and the panes ending at or before it are dropped.
    pub fn evict_through(&mut self, metric: &str, oldest_kept_ms: i64) {
        if let Some(metric) = self.by_metric.get_mut(metric) {
            metric.panes = metric.panes.split_off(&oldest_kept_ms.saturating_add(1));
            metric.evicted_through_ms = metric.evicted_through_ms.max(Some(oldest_kept_ms));
        }
    }

    fn chunks(&self) -> impl Iterator<Item = &Arc<Chunk>> {
        self.by_metric
            .values()
            .flat_map(|metric| metric.panes.values())
            .flatten()
    }

    /// Panes held, over every metric.
    pub fn panes(&self) -> usize {
        self.by_metric.values().map(|m| m.panes.len()).sum()
    }

    /// Samples held, over every metric.
    pub fn len(&self) -> usize {
        self.chunks().map(|chunk| chunk.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes of the samples held.
    pub fn bytes(&self) -> usize {
        self.chunks().map(|chunk| chunk.bytes()).sum()
    }

    /// `metric`'s samples in `time_range_ms` (`(start, end]`, every sample
    /// when `None`) as batches of `request`'s schema: one per chunk of the
    /// panes overlapping the range, the open pane's included. A chunk read
    /// whole is converted once per `reads` and shared by its later reads.
    fn read(
        &self,
        metric: &str,
        request: &RawDataRequest,
        reads: Option<&RawReads>,
        partition: Option<&PartitionReads>,
    ) -> Result<Vec<Batch>, Error> {
        let columns = column_kinds(metric, request)?;
        let Some(metric_data) = self.by_metric.get(metric) else {
            return Ok(Vec::new());
        };
        // A range starting before the evicted samples cannot be read whole:
        // the read fails, as a missing view window does. A read of every
        // sample returns the kept ones.
        let evicted_through = metric_data.evicted_through_ms.unwrap_or(i64::MIN);
        let (start, end) = request.time_range_ms.unwrap_or((evicted_through, i64::MAX));
        if start < evicted_through {
            return Err(Error::Invalid(format!(
                "the raw samples of {metric} at or before {evicted_through} are evicted, so ({start}, {end}] cannot be read whole"
            )));
        }
        if start >= end {
            return Ok(Vec::new());
        }
        // Pane `(e - w, e]` overlaps `(start, end]` when
        // `start < e < end + w`.
        let panes = metric_data.panes.range((
            Bound::Excluded(start),
            Bound::Excluded(end.saturating_add(self.pane_width_ms)),
        ));
        let mut batches = Vec::new();
        for stored in panes.flat_map(|(_, pane)| pane) {
            let Some(chunk) = stored.in_range((start, end)) else {
                continue;
            };
            let Some(chunk) = (match partition {
                Some(partition) if Arc::ptr_eq(stored, &chunk) => partition.chunk(stored),
                Some(partition) => chunk.in_partition(partition.partition),
                None => Some(chunk),
            }) else {
                continue;
            };
            let convert = || -> Result<Batch, Error> {
                let columns = columns
                    .iter()
                    .map(|kind| chunk.column(kind).map(BatchColumn::Plain))
                    .collect::<Result<Vec<_>, Error>>()?;
                Batch::try_from_columns(request.schema.clone(), columns)
            };
            let batch = match reads {
                Some(reads) if Arc::ptr_eq(stored, &chunk) => {
                    reads.get_or_convert(stored, request.schema, convert)?
                }
                _ => convert()?,
            };
            batches.push(batch);
        }
        Ok(batches)
    }
}

/// One of `count` disjoint parts of the series, by series id: a window's
/// ingestion run split by series reads one part per run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesPartition {
    pub index: u32,
    pub count: u32,
}

impl SeriesPartition {
    fn holds(&self, series_id: u32) -> bool {
        series_id % self.count == self.index
    }
}

/// One run's part of the series and the stored chunks it has cut to that
/// part, so the run's scans filter each chunk (and build its series
/// identities) once, as a whole run reads each chunk's cached ones.
pub(crate) struct PartitionReads {
    partition: SeriesPartition,
    /// By chunk address; each entry holds the chunk, so its address is not
    /// reused while it is kept.
    chunks: Mutex<HashMap<usize, CutChunk>>,
}

/// A stored chunk and its rows of one part of the series (`None`: none).
type CutChunk = (Arc<Chunk>, Option<Arc<Chunk>>);

impl PartitionReads {
    pub(crate) fn new(partition: SeriesPartition) -> Self {
        Self {
            partition,
            chunks: Mutex::default(),
        }
    }

    fn chunk(&self, stored: &Arc<Chunk>) -> Option<Arc<Chunk>> {
        let key = Arc::as_ptr(stored) as usize;
        let mut chunks = self.chunks.lock().unwrap_or_else(|e| e.into_inner());
        chunks
            .entry(key)
            .or_insert_with(|| (stored.clone(), stored.in_partition(self.partition)))
            .1
            .clone()
    }
}

/// Raw-data chunks already converted to batches, for the evaluations of
/// one range request: every step reads the same snapshot, and a chunk read
/// whole by several steps is converted once (its label columns built,
/// its series identities attached) rather than once per step.
pub struct RawReads {
    /// By chunk and request schema, in shards by chunk so the query
    /// workers rarely wait for each other; each entry holds the chunk and
    /// the schema, so their addresses are not reused while it is kept.
    shards: Vec<Mutex<ConvertedChunks>>,
}

type ConvertedChunks = HashMap<(usize, usize), (Arc<Chunk>, SchemaRef, Batch)>;

impl Default for RawReads {
    fn default() -> Self {
        Self {
            shards: (0..64).map(|_| Mutex::default()).collect(),
        }
    }
}

impl RawReads {
    fn get_or_convert(
        &self,
        chunk: &Arc<Chunk>,
        schema: &SchemaRef,
        convert: impl FnOnce() -> Result<Batch, Error>,
    ) -> Result<Batch, Error> {
        let key = (Arc::as_ptr(chunk) as usize, Arc::as_ptr(schema) as usize);
        // Chunks are at least 8-byte aligned: shard by the bits above.
        let shard = &self.shards[(key.0 >> 4) % self.shards.len()];
        if let Some((_, _, batch)) = shard.lock().unwrap_or_else(|e| e.into_inner()).get(&key) {
            return Ok(batch.clone());
        }
        // Converted outside the lock; a step converting the same chunk at
        // the same time keeps the first batch stored.
        let batch = convert()?;
        Ok(shard
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key)
            .or_insert_with(|| (chunk.clone(), schema.clone(), batch))
            .2
            .clone())
    }
}

/// What one column of a time series table holds.
#[derive(Debug, Clone, PartialEq)]
enum ColumnKind {
    Time,
    Value,
    /// The canonical JSON of every label, `__name__` included.
    SeriesIdentity,
    /// One label's value, empty when the series lacks it.
    Label(String),
}

/// What each column of `request`'s schema holds.
fn column_kinds(metric: &str, request: &RawDataRequest) -> Result<Vec<ColumnKind>, Error> {
    request
        .schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if Some(index) == request.schema.time_index {
                Ok(ColumnKind::Time)
            } else if field.name == PROMQL_SERIES_IDENTITY {
                Ok(ColumnKind::SeriesIdentity)
            } else if field.name == "value"
                && field.dtype == FieldDataType::Plain(DataType::Float64)
            {
                Ok(ColumnKind::Value)
            } else if field.dtype == FieldDataType::Plain(DataType::Utf8) {
                Ok(ColumnKind::Label(field.name.clone()))
            } else {
                Err(Error::Invalid(format!(
                    "time series {metric} has no column {} of type {:?}",
                    field.name, field.dtype
                )))
            }
        })
        .collect()
}

/// The raw data of every ingest shard: each shard holds the samples of
/// its series; a read takes the union.
pub type RawShards = Arc<Vec<Arc<RawData>>>;

/// What one run reads: the store as it was when the run started. It
/// records the first view or raw read that found its rows missing (a view
/// window not stored, raw samples evicted), which says the query cannot be
/// answered from what is stored (rather than that it failed).
pub struct RunStore {
    raw_data: RawShards,
    views: Arc<ViewStore>,
    /// Chunks converted by the other evaluations of the same range request.
    raw_reads: Option<Arc<RawReads>>,
    /// The series the run reads raw samples of; `None`: every series.
    partition: Option<PartitionReads>,
    /// The query-time results the run reads, by node: each computed for
    /// the run's evaluation time before the run starts and held by the run.
    query_time_results: HashMap<PhysicalASAPNodeId, Arc<Vec<Batch>>>,
    missing_rows: Mutex<Option<String>>,
}

impl RunStore {
    pub fn new(raw_data: RawShards, views: Arc<ViewStore>) -> Self {
        Self::with_query_time_results(raw_data, views, None, HashMap::new())
    }

    /// A run reading `query_time_results` besides the views and raw data,
    /// sharing converted raw chunks through `raw_reads`.
    pub fn with_query_time_results(
        raw_data: RawShards,
        views: Arc<ViewStore>,
        raw_reads: Option<Arc<RawReads>>,
        query_time_results: HashMap<PhysicalASAPNodeId, Arc<Vec<Batch>>>,
    ) -> Self {
        Self {
            raw_data,
            views,
            raw_reads,
            partition: None,
            query_time_results,
            missing_rows: Mutex::new(None),
        }
    }

    /// A run reading the raw samples of `partition`'s series only.
    pub fn for_partition(
        raw_data: RawShards,
        views: Arc<ViewStore>,
        partition: Option<SeriesPartition>,
    ) -> Self {
        Self {
            partition: partition.map(PartitionReads::new),
            ..Self::new(raw_data, views)
        }
    }

    /// Why the run's first view or raw read failed, if one did.
    pub fn missing_rows(&self) -> Option<String> {
        self.missing_rows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn record_missing(&self, error: &Error) {
        self.missing_rows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| error.to_string());
    }
}

impl DataStore for RunStore {
    fn read_raw_data(
        &self,
        source: &Source,
        request: &RawDataRequest,
        context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        let Source::TimeSeries { metric } = source else {
            return Err(Error::Invalid(format!(
                "the data plane stores time series only, not {source:?}"
            )));
        };
        let mut batches = Vec::new();
        for shard in self.raw_data.iter() {
            let read = shard.read(
                metric,
                request,
                self.raw_reads.as_deref(),
                self.partition.as_ref(),
            );
            batches.extend(read.inspect_err(|error| self.record_missing(error))?);
        }
        Ok(stream::iter(batches)
            .map(move |batch| {
                if context.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                let _reservation = context.reserve(batch.memory_bytes())?;
                Ok(batch)
            })
            .boxed_local())
    }

    fn read_materialized_view(
        &self,
        view: &MaterializedView,
        request: &MaterializedViewRequest,
        context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        // A query-time result is the node's whole output at the run's
        // evaluation time, whatever range the request names.
        if let Some(batches) = self.query_time_results.get(&view.node_id) {
            return Ok(stream::iter(batches.iter().cloned())
                .map(move |batch| {
                    if context.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    Ok(batch)
                })
                .boxed_local());
        }
        self.views
            .read(view, request, context)
            .inspect_err(|error| self.record_missing(error))
    }
}

/// The end at or before which `view`'s windows are past its retention,
/// counted back from the window ending at `newest_end`.
pub(crate) fn oldest_kept_end(view: &MaterializedView, newest_end: i64) -> i64 {
    newest_end.saturating_sub(crate::ingestion::millis(view.retention_ms()))
}

/// Rows stored for one view: one entry per appended window, oldest first.
type ViewRows = Vec<ViewWindow>;

/// One window of a view's rows: the `(start, end]` they cover, their
/// batches, and the bytes they hold apart from the view's shared texts.
#[derive(Clone)]
struct ViewWindow {
    time_range_ms: (i64, i64),
    batches: Arc<Vec<Batch>>,
    /// Counted when the window is appended: a run that later reads its
    /// states marks them held outside the run, after which
    /// [`Batch::memory_bytes`] no longer counts them.
    bytes: usize,
}

/// The texts of one text column of one view, each stored once: every
/// window's dictionary column of that view has this one values array, and
/// a window holds only its rows' keys into it. Append-only: a text stays
/// for as long as the data plane runs, as the series interner keeps its
/// series.
#[derive(Default)]
struct SharedTexts {
    code_of: HashMap<Arc<str>, u32>,
    texts: Vec<Arc<str>>,
    /// The values array the windows share, rebuilt when a text is added.
    values: Option<ArrayRef>,
}

impl SharedTexts {
    /// `column` re-keyed into the shared values; `None` when it is not a
    /// dictionary of texts.
    fn share(&mut self, column: &ArrayRef) -> Result<Option<ArrayRef>, Error> {
        let Some(dictionary) = column
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
        else {
            return Ok(None);
        };
        let Some(own_values) = dictionary.values().as_any().downcast_ref::<StringArray>() else {
            return Ok(None);
        };
        if self
            .values
            .as_ref()
            .is_some_and(|values| Arc::ptr_eq(values, dictionary.values()))
        {
            return Ok(Some(column.clone()));
        }
        // The window's own entries, mapped to shared codes once each.
        let mut added = false;
        let mut code_of_entry: Vec<Option<u32>> = vec![None; own_values.len()];
        let keys = dictionary.keys();
        let mut shared_keys = Vec::with_capacity(keys.len());
        for row in 0..keys.len() {
            if keys.is_null(row) {
                shared_keys.push(None);
                continue;
            }
            let entry = keys.value(row) as usize;
            let code = match code_of_entry[entry] {
                Some(code) => code,
                None => {
                    let text = own_values.value(entry);
                    let code = match self.code_of.get(text) {
                        Some(&code) => code,
                        None => {
                            let code = u32::try_from(self.texts.len()).map_err(|_| {
                                Error::Invalid("more than u32::MAX texts in one view".into())
                            })?;
                            let text: Arc<str> = text.into();
                            self.texts.push(text.clone());
                            self.code_of.insert(text, code);
                            added = true;
                            code
                        }
                    };
                    code_of_entry[entry] = Some(code);
                    code
                }
            };
            shared_keys.push(Some(code));
        }
        if added || self.values.is_none() {
            self.values = Some(Arc::new(StringArray::from_iter_values(
                self.texts.iter().map(|text| &**text),
            )));
        }
        let values = self.values.clone().expect("set above");
        Ok(Some(Arc::new(
            DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(shared_keys), values)
                .map_err(|e| Error::Invalid(e.to_string()))?,
        )))
    }

    /// Bytes of the shared values (counted once per view column).
    fn bytes(&self) -> usize {
        self.values
            .as_ref()
            .map_or(0, |values| values.get_array_memory_size())
    }
}

/// Each materialized view's stored rows. Cloning shares every batch, so a
/// writer copies only the per-view lists when a run still reads them; the
/// shared texts are shared by every clone (only the precompute thread
/// appends).
#[derive(Default, Clone)]
pub struct ViewStore {
    by_view: BTreeMap<PhysicalASAPNodeId, ViewRows>,
    /// Per view, window part and text column index, the texts its windows
    /// share. A window split by series keeps one batch per part (the
    /// parts' series are stable across windows), and each part has its own
    /// texts: a run charges each batch it reads for its buffers, the shared
    /// values included, so one view-wide array in every part's batch would
    /// be charged once per part per window.
    texts: BTreeMap<(PhysicalASAPNodeId, usize, usize), Arc<Mutex<SharedTexts>>>,
    /// For a view another refresh group reads (a narrower level of a window
    /// chain, read by `SummaryMergeWindows`): per reader group, the end of
    /// its newest committed window. The view keeps its rows ending after
    /// the oldest of these whatever its retention: the plan sizes the
    /// retention for a reader that runs right behind it, and the precompute
    /// pool lets a group run ahead of its readers.
    readers: BTreeMap<PhysicalASAPNodeId, BTreeMap<usize, i64>>,
}

impl ViewStore {
    /// Registers `readers`, the refresh groups that read `view`, as having
    /// committed nothing yet: until each commits, `view` keeps every row.
    pub fn hold_for_readers(&mut self, view: PhysicalASAPNodeId, readers: &[usize]) {
        let held = self.readers.entry(view).or_default();
        for &reader in readers {
            held.entry(reader).or_insert(i64::MIN);
        }
    }

    /// Reader group `reader` committed its window ending `end_ms`, reading
    /// `view`: rows of `view` ending at or before it are no longer held.
    pub fn reader_committed(&mut self, view: PhysicalASAPNodeId, reader: usize, end_ms: i64) {
        let through = self
            .readers
            .entry(view)
            .or_default()
            .entry(reader)
            .or_insert(end_ms);
        *through = (*through).max(end_ms);
    }

    /// Appends `batches` as `view`'s rows covering `time_range_ms`
    /// (`(start, end]`), then drops its rows ending `view.retention_ms()`
    /// or more before `end` (the newest are always kept), except those a
    /// reader group has yet to read ([`Self::hold_for_readers`]). Bad input when
    /// the range is empty or starts before the end of the rows appended
    /// last (the store is append-only). Each text column (a series
    /// identity or a label) is stored with the view's shared texts, so a
    /// text is held once per view however many windows it is in.
    pub fn append(
        &mut self,
        view: &MaterializedView,
        time_range_ms: (i64, i64),
        batches: Vec<Batch>,
    ) -> Result<(), Error> {
        let (start, end) = time_range_ms;
        if start >= end
            || self
                .by_view
                .get(&view.node_id)
                .and_then(|rows| rows.last())
                .is_some_and(|window| start < window.time_range_ms.1)
        {
            return Err(Error::Invalid(format!(
                "materialized view {}: appended rows ({start}, {end}] are empty or not after the stored ones",
                view.node_id
            )));
        }
        let batches = batches
            .into_iter()
            .enumerate()
            .map(|(part, batch)| self.share_texts(view.node_id, part, batch))
            .collect::<Result<Vec<_>, Error>>()?;
        let bytes = batches.iter().map(window_batch_bytes).sum();
        let mut oldest_kept_end = oldest_kept_end(view, end);
        if let Some(&held) = self
            .readers
            .get(&view.node_id)
            .and_then(|r| r.values().min())
        {
            oldest_kept_end = oldest_kept_end.min(held);
        }
        let rows = self.by_view.entry(view.node_id).or_default();
        rows.push(ViewWindow {
            time_range_ms,
            batches: Arc::new(batches),
            bytes,
        });
        rows.retain(|window| {
            let row_end = window.time_range_ms.1;
            row_end > oldest_kept_end || row_end == end
        });
        Ok(())
    }

    /// `batch`, part `part` of its window, with each text column keyed into
    /// that part's shared texts of `view`.
    fn share_texts(
        &mut self,
        view: PhysicalASAPNodeId,
        part: usize,
        batch: Batch,
    ) -> Result<Batch, Error> {
        let schema = batch.schema().clone();
        let mut changed = false;
        let mut columns = Vec::with_capacity(schema.fields.len());
        for (index, field) in schema.fields.iter().enumerate() {
            if let Some(states) = batch.state_column(index) {
                let indices = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| Error::Invalid("state indices are not u32".into()))?
                    .clone();
                columns.push(BatchColumn::States {
                    indices,
                    states: states.clone(),
                });
                continue;
            }
            let column = batch.column(index);
            let shared = if field.dtype == FieldDataType::Plain(DataType::Utf8) {
                let texts = self.texts.entry((view, part, index)).or_default().clone();
                let mut texts = texts.lock().unwrap_or_else(|e| e.into_inner());
                texts.share(column)?
            } else {
                None
            };
            match shared {
                Some(array) => {
                    changed |= !Arc::ptr_eq(&array, column);
                    columns.push(BatchColumn::Plain(array));
                }
                None => columns.push(BatchColumn::Plain(column.clone())),
            }
        }
        if !changed {
            return Ok(batch);
        }
        Batch::try_from_columns(schema, columns)
    }

    /// The end of `view`'s newest stored rows.
    pub fn stored_through(&self, view: PhysicalASAPNodeId) -> Option<i64> {
        self.by_view
            .get(&view)?
            .last()
            .map(|window| window.time_range_ms.1)
    }

    /// Bytes of the stored rows: each window as counted when it was
    /// appended (states included, whether or not a run has read them
    /// since), plus each view's shared texts once.
    pub fn bytes(&self) -> usize {
        let windows: usize = self
            .by_view
            .values()
            .flatten()
            .map(|window| window.bytes)
            .sum();
        let texts: usize = self
            .texts
            .values()
            .map(|texts| texts.lock().unwrap_or_else(|e| e.into_inner()).bytes())
            .sum();
        windows + texts
    }

    /// `view`'s rows whose ranges tile `request.time_range_ms` exactly;
    /// fails when one is missing.
    fn read(
        &self,
        view: &MaterializedView,
        request: &MaterializedViewRequest,
        context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        let (start, end) = request.time_range_ms;
        let missing = || {
            Error::Invalid(format!(
                "materialized view {} has no rows tiling ({start}, {end}]",
                view.node_id
            ))
        };
        let rows = self.by_view.get(&view.node_id).ok_or_else(missing)?;
        // Rows are disjoint and oldest first, so the rows inside the range
        // are one slice: from the first starting at or after `start` to
        // the last ending at or before `end`.
        let first = rows.partition_point(|window| window.time_range_ms.0 < start);
        let past_last = rows.partition_point(|window| window.time_range_ms.1 <= end);
        let selected: &[ViewWindow] = rows.get(first..past_last).unwrap_or_default();
        // The selected ranges, oldest first, must tile `(start, end]`.
        let mut covered_to = start;
        for window in selected {
            let (row_start, row_end) = window.time_range_ms;
            if row_start != covered_to {
                return Err(missing());
            }
            covered_to = row_end;
        }
        if covered_to != end {
            return Err(missing());
        }
        Ok(stream::iter(
            selected
                .iter()
                .flat_map(|window| window.batches.iter().cloned()),
        )
        .map(move |batch| {
            if context.is_cancelled() {
                return Err(Error::Cancelled);
            }
            Ok(batch)
        })
        .boxed_local())
    }
}

/// A stored window batch's bytes apart from its shared texts: its own
/// buffers and states ([`Batch::memory_bytes`]) less each text column's
/// values array, which the view's [`SharedTexts`] counts once.
fn window_batch_bytes(batch: &Batch) -> usize {
    let shared: usize = (0..batch.schema().fields.len())
        .filter(|&index| batch.state_column(index).is_none())
        .filter_map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .map(|dictionary| dictionary.values().get_array_memory_size())
        })
        .sum();
    batch.memory_bytes().saturating_sub(shared)
}

/// The newest published snapshot of the store, which every query run reads.
pub struct PublishedStore {
    snapshot: RwLock<(RawShards, Arc<ViewStore>)>,
    /// The watermark as of the raw data published, `i64::MIN` before the
    /// first: no sample at or before it is expected any more.
    watermark_ms: std::sync::atomic::AtomicI64,
}

impl PublishedStore {
    pub fn new(raw_data: RawShards) -> Self {
        Self {
            snapshot: RwLock::new((raw_data, Arc::default())),
            watermark_ms: std::sync::atomic::AtomicI64::new(i64::MIN),
        }
    }

    /// Makes `raw_data`, complete through `watermark_ms`, what new runs
    /// read.
    pub fn publish_raw_data(&self, raw_data: RawShards, watermark_ms: Option<i64>) {
        self.snapshot.write().unwrap_or_else(|e| e.into_inner()).0 = raw_data;
        if let Some(watermark_ms) = watermark_ms {
            self.watermark_ms.fetch_max(watermark_ms, Ordering::SeqCst);
        }
    }

    /// The watermark as of the raw data published; `None` before the
    /// first.
    pub fn watermark_ms(&self) -> Option<i64> {
        Some(self.watermark_ms.load(Ordering::SeqCst)).filter(|&w| w != i64::MIN)
    }

    /// Makes `views` what new runs read.
    pub fn publish_views(&self, views: Arc<ViewStore>) {
        self.snapshot.write().unwrap_or_else(|e| e.into_inner()).1 = views;
    }

    /// The newest published raw data and views.
    pub fn snapshot(&self) -> (RawShards, Arc<ViewStore>) {
        self.snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// One query-time result's state.
enum ResultState<E> {
    /// Its first reader is computing it; others wait on the entry.
    Computing,
    /// Computed: the rows and their bytes.
    Done(Arc<Vec<Batch>>, usize),
    /// Its computation failed; the readers that waited get the error.
    Failed(E),
}

/// One query-time result and the condition its waiters wait on.
type QueryTimeEntry<E> = Arc<(Mutex<ResultState<E>>, Condvar)>;

/// The materialized view store's query-time partition: the output of each
/// query-time `OneEvaluation` node at one evaluation time `T`, shared by
/// every query run at `T` that reads it, keyed by `(T, node)`. Query
/// workers write it during their runs, so unlike the ingestion-time views
/// it is not a published snapshot. A missing result is computed by the
/// first run that needs it while the others wait for it (single flight);
/// a failure reaches the runs that waited and is not kept. Only a result
/// at a time the watermark has passed is kept (its inputs are complete);
/// one at a later time is computed by each run that needs it. A result is
/// kept while `T` is within `retention_ms` of the watermark, and the
/// oldest are dropped while more than `max_bytes` are held.
pub struct QueryTimeResults<E = crate::query::Unanswered> {
    entries: Mutex<BTreeMap<(i64, PhysicalASAPNodeId), QueryTimeEntry<E>>>,
    retention_ms: i64,
    max_bytes: usize,
    bytes: AtomicU64,
    computes: AtomicU64,
    hits: AtomicU64,
}

impl<E: Clone> QueryTimeResults<E> {
    pub fn new(retention_ms: i64, max_bytes: usize) -> Self {
        Self {
            entries: Mutex::default(),
            retention_ms,
            max_bytes,
            bytes: AtomicU64::new(0),
            computes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    /// `node`'s output at `time_ms`: stored, or computed now by `compute`
    /// (once, however many runs ask at once, when `time_ms` is at or
    /// before `watermark_ms`; by each run otherwise).
    pub fn get_or_compute(
        &self,
        node: PhysicalASAPNodeId,
        time_ms: i64,
        watermark_ms: Option<i64>,
        compute: impl FnOnce() -> Result<Vec<Batch>, E>,
    ) -> Result<Arc<Vec<Batch>>, E> {
        if watermark_ms.is_none_or(|watermark| time_ms > watermark) {
            self.computes.fetch_add(1, Ordering::Relaxed);
            return compute().map(Arc::new);
        }
        let (entry, computer) = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            match entries.get(&(time_ms, node)) {
                Some(entry) => (entry.clone(), false),
                None => {
                    let entry: QueryTimeEntry<E> =
                        Arc::new((Mutex::new(ResultState::Computing), Condvar::new()));
                    entries.insert((time_ms, node), entry.clone());
                    (entry, true)
                }
            }
        };
        let (state, done) = &*entry;
        if !computer {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            while matches!(*state, ResultState::Computing) {
                state = done.wait(state).unwrap_or_else(|e| e.into_inner());
            }
            self.hits.fetch_add(1, Ordering::Relaxed);
            return match &*state {
                ResultState::Done(batches, _) => Ok(batches.clone()),
                ResultState::Failed(error) => Err(error.clone()),
                ResultState::Computing => unreachable!("waited above"),
            };
        }
        self.computes.fetch_add(1, Ordering::Relaxed);
        let result = compute().map(Arc::new);
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let held = entries
            .get(&(time_ms, node))
            .is_some_and(|held| Arc::ptr_eq(held, &entry));
        {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            *state = match &result {
                Ok(batches) => {
                    let bytes = batches.iter().map(Batch::memory_bytes).sum::<usize>();
                    if held {
                        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
                    }
                    ResultState::Done(batches.clone(), bytes)
                }
                Err(error) => ResultState::Failed(error.clone()),
            };
            done.notify_all();
        }
        if result.is_err() && held {
            entries.remove(&(time_ms, node));
        }
        if let Some(watermark_ms) = watermark_ms {
            self.drop_old(&mut entries, watermark_ms);
        }
        result
    }

    /// Drops the results more than the retention behind `watermark_ms`,
    /// then the oldest while more than `max_bytes` are held; never one
    /// being computed (its computer finds it gone and does not count it).
    fn drop_old(
        &self,
        entries: &mut BTreeMap<(i64, PhysicalASAPNodeId), QueryTimeEntry<E>>,
        watermark_ms: i64,
    ) {
        let oldest_kept_ms = watermark_ms.saturating_sub(self.retention_ms);
        let keys: Vec<(i64, PhysicalASAPNodeId)> = entries.keys().copied().collect();
        for key in keys {
            let expired = key.0 < oldest_kept_ms;
            let over = self.bytes.load(Ordering::Relaxed) as usize > self.max_bytes;
            if !expired && !over {
                break;
            }
            let bytes = {
                let Ok(state) = entries[&key].0.try_lock() else {
                    continue;
                };
                match &*state {
                    ResultState::Computing => continue,
                    ResultState::Done(_, bytes) => *bytes,
                    ResultState::Failed(_) => 0,
                }
            };
            entries.remove(&key);
            self.bytes.fetch_sub(bytes as u64, Ordering::Relaxed);
        }
    }

    /// Bytes of the results held.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Results held, computed or being computed.
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Results computed so far (misses), kept or not.
    pub fn computes(&self) -> u64 {
        self.computes.load(Ordering::Relaxed)
    }

    /// Reads served by a result another run computed, waiting for it if
    /// needed.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

/// Every metric a `Scan` of `nodes` reads.
pub fn scanned_metrics<'a>(
    nodes: impl IntoIterator<
        Item = &'a asap_executor::planner::plan::physical_asap_dag::PhysicalASAPDAGNode,
    >,
) -> BTreeSet<String> {
    use asap_executor::planner::ir::{NonASAPOp, Operator};
    nodes
        .into_iter()
        .filter_map(|node| match &node.payload {
            Operator::NonASAP(NonASAPOp::Scan {
                source: Source::TimeSeries { metric },
                ..
            }) => Some(metric.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_executor::planner::ir::schema::{Field, Schema};
    use asap_executor::values::Value;

    use crate::series_ids::intern_pairs;

    /// The rows the row-wise store this one replaced read: every retained
    /// sample of the metric in `(start, end]`, in arrival order, with the
    /// series identity serialized per series.
    fn row_read(
        samples: &[RawSample],
        schema: &Schema,
        range: Option<(i64, i64)>,
    ) -> Vec<Vec<Value>> {
        samples
            .iter()
            .filter(|s| {
                range.is_none_or(|(start, end)| start < s.timestamp_ms && s.timestamp_ms <= end)
            })
            .map(|sample| {
                schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        if Some(index) == schema.time_index {
                            Value::Timestamp(sample.timestamp_ms)
                        } else if field.name == PROMQL_SERIES_IDENTITY {
                            let labels: &BTreeMap<String, String> = &sample.labels;
                            Value::Utf8(serde_json::to_string(labels).unwrap().into())
                        } else if field.name == "value" {
                            Value::Float64(sample.value)
                        } else {
                            Value::Utf8(
                                sample
                                    .labels
                                    .get(&field.name)
                                    .cloned()
                                    .unwrap_or_default()
                                    .into(),
                            )
                        }
                    })
                    .collect()
            })
            .collect()
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::with_time_index(
            vec![
                Field::plain("ts", DataType::Timestamp, false),
                Field::plain("value", DataType::Float64, false),
                Field::plain(PROMQL_SERIES_IDENTITY, DataType::Utf8, false),
                Field::plain("job", DataType::Utf8, false),
            ],
            0,
            vec![],
        ))
    }

    /// Samples of `m_store_test` from four series (two sharing a `job`, one
    /// without one) every 700 ms over 30 s, sent in requests of 3 s with
    /// series interleaved, plus one late sample.
    fn samples() -> Vec<Vec<RawSample>> {
        let series = [
            intern_pairs(&[("__name__", "m_store_test"), ("job", "a"), ("i", "1")]),
            intern_pairs(&[("__name__", "m_store_test"), ("job", "a"), ("i", "2")]),
            intern_pairs(&[("__name__", "m_store_test"), ("job", "b"), ("i", "3")]),
            intern_pairs(&[("__name__", "m_store_test"), ("i", "4")]),
        ];
        let mut requests: Vec<Vec<RawSample>> = Vec::new();
        for t in (0..30_000).step_by(700) {
            if t % 3_000 < 700 {
                requests.push(Vec::new());
            }
            for (index, series) in series.iter().enumerate() {
                requests.last_mut().unwrap().push(RawSample {
                    labels: series.clone(),
                    timestamp_ms: t + index as i64,
                    value: (t / 100 + index as i64) as f64,
                });
            }
        }
        requests.push(vec![RawSample {
            labels: series[0].clone(),
            timestamp_ms: 4_242,
            value: -1.0,
        }]);
        requests
    }

    fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<String> {
        let mut text: Vec<String> = rows.drain(..).map(|row| format!("{row:?}")).collect();
        text.sort();
        text
    }

    fn read_rows(
        raw: &RawData,
        schema: &Arc<Schema>,
        range: Option<(i64, i64)>,
        reads: Option<&RawReads>,
    ) -> Vec<Vec<Value>> {
        let request = RawDataRequest {
            time_range_ms: range,
            schema,
            predicates: &[],
        };
        raw.read("m_store_test", &request, reads, None)
            .unwrap()
            .iter()
            .flat_map(Batch::to_rows)
            .collect()
    }

    /// A columnar read returns the rows the row-wise read returned, for
    /// ranges inside one pane, across panes (the open one included) and
    /// unbounded, before and after panes are compacted; and so do reads
    /// that share converted chunks, as a range request's steps do.
    #[test]
    fn columnar_read_returns_the_row_read_rows() {
        let schema = schema();
        let requests = samples();
        let all: Vec<RawSample> = requests.iter().flatten().cloned().collect();
        let mut raw = RawData::for_metrics(&["m_store_test".to_string()], 5_000);
        for request in &requests {
            raw.append(request);
        }
        let ranges = [
            None,
            Some((5_000, 10_000)),
            Some((4_000, 4_242)),
            Some((2_100, 27_001)),
            Some((25_000, 40_000)),
            Some((-10, 0)),
        ];
        for sealed in [false, true] {
            if sealed {
                raw.seal_through(20_000);
            }
            let shared = RawReads::default();
            for range in ranges {
                for reads in [None, Some(&shared), Some(&shared)] {
                    assert_eq!(
                        sorted(read_rows(&raw, &schema, range, reads)),
                        sorted(row_read(&all, &schema, range)),
                        "range {range:?}, sealed {sealed}, shared {}",
                        reads.is_some()
                    );
                }
            }
            let converted: usize = shared.shards.iter().map(|s| s.lock().unwrap().len()).sum();
            assert_eq!(converted, raw.chunks().count(), "each chunk converted once");
        }
        // A closed pane is compacted into one chunk; the open ones are not.
        let panes = &raw.by_metric["m_store_test"].panes;
        assert_eq!(panes[&5_000].len(), 1);
        assert!(panes[&25_000].len() > 1);
    }

    /// Eviction drops the panes ending at or before the cutoff, keeps the
    /// pane the cutoff falls in, and no read returns a sample at or
    /// before the cutoff: a read of a range starting before it fails (its
    /// window would be partial), a read of every sample returns the kept
    /// ones.
    #[test]
    fn evicts_whole_panes() {
        let schema = schema();
        let all: Vec<RawSample> = samples().into_iter().flatten().collect();
        let mut raw = RawData::for_metrics(&["m_store_test".to_string()], 5_000);
        raw.append(&all);
        let bytes_before = raw.bytes();
        raw.evict_through("m_store_test", 12_000);
        let ends: Vec<i64> = raw.by_metric["m_store_test"]
            .panes
            .keys()
            .copied()
            .collect();
        assert_eq!(ends, [15_000, 20_000, 25_000, 30_000]);
        assert!(raw.bytes() < bytes_before);
        let kept: Vec<RawSample> = all
            .iter()
            .filter(|s| s.timestamp_ms > 12_000)
            .cloned()
            .collect();
        for range in [None, Some((12_000, 14_000))] {
            assert_eq!(
                sorted(read_rows(&raw, &schema, range, None)),
                sorted(row_read(&kept, &schema, range)),
            );
        }
        let request = RawDataRequest {
            time_range_ms: Some((0, 14_000)),
            schema: &schema,
            predicates: &[],
        };
        assert!(raw.read("m_store_test", &request, None, None).is_err());
    }

    /// A view read returns exactly the stored windows that tile the
    /// requested range, and fails when they do not.
    #[test]
    fn view_reads_select_the_windows_tiling_the_range() {
        let view: MaterializedView = serde_json::from_value(serde_json::json!({
            "node_id": 1,
            "refresh": "on_first_read",
            "coverage": {"definition_id": "test-view", "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap();
        let mut views = ViewStore::default();
        for end in [10, 20, 30, 40] {
            views.append(&view, (end - 10, end), Vec::new()).unwrap();
        }
        let context = RunContext::new(
            asap_executor::runtime::Scope::Query {
                evaluation_time_ms: 40,
            },
            asap_executor::runtime::Limits::default(),
        )
        .unwrap();
        let schema = schema();
        for (range, tiled) in [
            ((10, 30), true),
            ((0, 40), true),
            ((30, 40), true),
            ((5, 30), false),
            ((10, 35), false),
            ((40, 50), false),
            ((-10, 0), false),
        ] {
            let request = MaterializedViewRequest {
                time_range_ms: range,
                schema: &schema,
            };
            assert_eq!(
                views.read(&view, &request, context.clone()).is_ok(),
                tiled,
                "{range:?}"
            );
        }
    }

    /// Every window of a view keys its text columns into one shared values
    /// array: a series identity is stored once per view, the rows read back
    /// unchanged, and a series first seen in a later window is added
    /// without changing the earlier windows' rows.
    #[test]
    fn view_windows_share_one_copy_of_each_text() {
        let view: MaterializedView = serde_json::from_value(serde_json::json!({
            "node_id": 1,
            "refresh": "on_first_read",
            "coverage": {"definition_id": "test-view", "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap();
        let schema = schema();
        // Identities of 200 bytes: one copy of 101 of them is 20 KB.
        let identity = |s: usize| {
            format!(
                r#"{{"__name__":"data","instance":"host-{s:04}","pad":"{}"}}"#,
                "x".repeat(155)
            )
        };
        let window = |end: i64, series: usize| -> Vec<Vec<Value>> {
            (0..series)
                .map(|s| {
                    vec![
                        Value::Timestamp(end),
                        Value::Float64(s as f64),
                        Value::Utf8(identity(s).into()),
                        Value::Utf8(format!("job-{}", s % 3).into()),
                    ]
                })
                .collect()
        };
        let mut views = ViewStore::default();
        let mut appended = Vec::new();
        for (end, series) in [(10, 100), (20, 100), (30, 101)] {
            let rows = window(end, series);
            views
                .append(
                    &view,
                    (end - 10, end),
                    vec![Batch::try_new(schema.clone(), rows.clone()).unwrap()],
                )
                .unwrap();
            appended.push(rows);
        }
        let stored = &views.by_view[&1];
        for (window, rows) in stored.iter().zip(&appended) {
            assert_eq!(
                format!("{:?}", window.batches[0].to_rows()),
                format!("{rows:?}")
            );
        }
        let values = |window: &ViewWindow, column: usize| {
            window.batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .unwrap()
                .values()
                .clone()
        };
        // The first two windows share one values array per text column;
        // the third, with a new series, holds the grown one.
        for column in [2, 3] {
            assert!(Arc::ptr_eq(
                &values(&stored[0], column),
                &values(&stored[1], column)
            ));
        }
        assert_eq!(values(&stored[2], 2).len(), 101);
        // A window adds its rows' columns and keys, not a copy of the
        // identities.
        let identities: usize = (0..101).map(|s| identity(s).len()).sum();
        let one_window = views.bytes();
        views
            .append(
                &view,
                (30, 40),
                vec![Batch::try_new(schema.clone(), window(40, 101)).unwrap()],
            )
            .unwrap();
        let added = views.bytes() - one_window;
        assert!(
            added < identities / 2,
            "a window added {added} bytes; identities are {identities}"
        );
    }

    /// A window split by series keeps one batch per part; each part's text
    /// columns share that part's texts across windows, holding only the
    /// part's own series, so a run charged per batch read is not charged a
    /// whole view's texts once per part.
    #[test]
    fn split_window_parts_keep_their_own_shared_texts() {
        let view: MaterializedView = serde_json::from_value(serde_json::json!({
            "node_id": 1,
            "refresh": "on_first_read",
            "coverage": {"definition_id": "test-view", "selection": []},
            "inputs": {"materialized_views": []},
            "retention_ms": 1_000_000,
        }))
        .unwrap();
        let schema = schema();
        let part = |end: i64, series: std::ops::Range<usize>| {
            let rows = series
                .map(|s| {
                    vec![
                        Value::Timestamp(end),
                        Value::Float64(s as f64),
                        Value::Utf8(format!("series-{s}").into()),
                        Value::Utf8(format!("job-{}", s % 3).into()),
                    ]
                })
                .collect();
            Batch::try_new(schema.clone(), rows).unwrap()
        };
        let mut views = ViewStore::default();
        for end in [10, 20] {
            views
                .append(
                    &view,
                    (end - 10, end),
                    vec![part(end, 0..50), part(end, 50..100)],
                )
                .unwrap();
        }
        let identities = |window: &ViewWindow, part: usize| {
            window.batches[part]
                .column(2)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .unwrap()
                .values()
                .clone()
        };
        let stored = &views.by_view[&1];
        for part in [0, 1] {
            assert_eq!(identities(&stored[0], part).len(), 50);
            assert!(Arc::ptr_eq(
                &identities(&stored[0], part),
                &identities(&stored[1], part)
            ));
        }
    }

    /// A query-time result at a time the watermark has passed is computed
    /// once and then read; one past the watermark is not kept; a failed
    /// computation is not kept; results more than the retention behind the
    /// watermark are dropped and their bytes released.
    #[test]
    fn query_time_results_are_computed_once_and_expire() {
        let results = QueryTimeResults::<()>::new(10_000, usize::MAX);
        let ok = || Ok::<_, ()>(Vec::new());
        let watermark = Some(1_000);
        results.get_or_compute(7, 1_000, watermark, ok).unwrap();
        results.get_or_compute(7, 1_000, watermark, ok).unwrap();
        assert_eq!((results.computes(), results.hits()), (1, 1));
        // Past the watermark (a client's far-future time): not kept.
        results.get_or_compute(7, 99_000, watermark, ok).unwrap();
        results.get_or_compute(7, 99_000, watermark, ok).unwrap();
        assert_eq!((results.computes(), results.len()), (3, 1));
        assert!(results
            .get_or_compute(8, 1_000, watermark, || Err(()))
            .is_err());
        assert_eq!(results.len(), 1);
        results.get_or_compute(8, 1_000, watermark, ok).unwrap();
        assert_eq!(results.len(), 2);
        // Retention counts back from the watermark, not from client times.
        results.get_or_compute(7, 11_000, Some(11_000), ok).unwrap();
        assert_eq!(results.len(), 3);
        results.get_or_compute(7, 11_001, Some(11_001), ok).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results.bytes(), 0);
    }

    /// Runs waiting for a result being computed get its value, or its
    /// failure, without computing it themselves.
    #[test]
    fn waiters_share_the_computers_result_or_failure() {
        for fails in [false, true] {
            let results = Arc::new(QueryTimeResults::<&'static str>::new(10_000, usize::MAX));
            let (started, wait_started) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel::<()>();
            let computer = {
                let results = results.clone();
                std::thread::spawn(move || {
                    results.get_or_compute(1, 0, Some(0), || {
                        started.send(()).unwrap();
                        released.recv().unwrap();
                        if fails {
                            Err("failed")
                        } else {
                            Ok(Vec::new())
                        }
                    })
                })
            };
            wait_started.recv().unwrap();
            let waiter = {
                let results = results.clone();
                std::thread::spawn(move || {
                    results.get_or_compute(1, 0, Some(0), || panic!("the waiter computed"))
                })
            };
            // Let the waiter block on the entry before the result is set.
            std::thread::sleep(std::time::Duration::from_millis(100));
            release.send(()).unwrap();
            let (first, second) = (computer.join().unwrap(), waiter.join().unwrap());
            assert_eq!(first.is_err(), fails);
            assert_eq!(second.is_err(), fails);
            assert_eq!(results.computes(), 1);
            assert_eq!(results.len(), usize::from(!fails));
        }
    }
}
