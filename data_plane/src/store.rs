//! The data plane's time-indexed store, which the executor reads through
//! `asap_executor::sources::DataStore`:
//!
//! - **raw data**: every Remote Write sample of a metric some `Scan` of the
//!   plan reads, as a time series table `(ts, value, *labels)`. A read
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
//! Each part has one writer (raw data: the ingest thread; views: the
//! precompute thread), which changes its own copy (copy on write: clones
//! share every chunk and batch) and publishes it ([`PublishedStore`]), so
//! a run never sees a change and never blocks one.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use asap_executor::arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray,
    UInt32Array,
};
use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::schema::{DataType, FieldDataType, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::runtime::{OutputStream, RunContext};
use asap_executor::sources::{DataStore, MaterializedViewRequest, RawDataRequest};
use asap_executor::values::{Batch, BatchColumn};
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
    /// panes overlapping the range, the open pane's included.
    fn read(&self, metric: &str, request: &RawDataRequest) -> Result<Vec<Batch>, Error> {
        let columns = column_kinds(metric, request)?;
        let Some(metric) = self.by_metric.get(metric) else {
            return Ok(Vec::new());
        };
        // Evicted samples are not read, as if they were gone.
        let (start, end) = request.time_range_ms.unwrap_or((i64::MIN, i64::MAX));
        let start = start.max(metric.evicted_through_ms.unwrap_or(i64::MIN));
        if start >= end {
            return Ok(Vec::new());
        }
        // Pane `(e - w, e]` overlaps `(start, end]` when
        // `start < e < end + w`.
        let panes = metric.panes.range((
            Bound::Excluded(start),
            Bound::Excluded(end.saturating_add(self.pane_width_ms)),
        ));
        let mut batches = Vec::new();
        for chunk in panes.flat_map(|(_, pane)| pane) {
            let Some(chunk) = chunk.in_range((start, end)) else {
                continue;
            };
            let columns = columns
                .iter()
                .map(|kind| chunk.column(kind).map(BatchColumn::Plain))
                .collect::<Result<Vec<_>, Error>>()?;
            batches.push(Batch::try_from_columns(request.schema.clone(), columns)?);
        }
        Ok(batches)
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

/// What one run reads: the store as it was when the run started. It
/// records the first view read that found no rows, which says the query
/// cannot be answered from what is stored (rather than that it failed).
pub struct RunStore {
    raw_data: Arc<RawData>,
    views: Arc<ViewStore>,
    /// The query-time results the run reads, by node: each computed for
    /// the run's evaluation time before the run starts and held by the run.
    query_time_results: HashMap<PhysicalASAPNodeId, Arc<Vec<Batch>>>,
    missing_view_rows: Mutex<Option<String>>,
}

impl RunStore {
    pub fn new(raw_data: Arc<RawData>, views: Arc<ViewStore>) -> Self {
        Self::with_query_time_results(raw_data, views, HashMap::new())
    }

    /// A run reading `query_time_results` besides the views and raw data.
    pub fn with_query_time_results(
        raw_data: Arc<RawData>,
        views: Arc<ViewStore>,
        query_time_results: HashMap<PhysicalASAPNodeId, Arc<Vec<Batch>>>,
    ) -> Self {
        Self {
            raw_data,
            views,
            query_time_results,
            missing_view_rows: Mutex::new(None),
        }
    }

    /// Why the run's first view read failed, if one did.
    pub fn missing_view_rows(&self) -> Option<String> {
        self.missing_view_rows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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
        let batches = self.raw_data.read(metric, request)?;
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
            .inspect_err(|error| {
                self.missing_view_rows
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert_with(|| error.to_string());
            })
    }
}

/// Rows stored for one view: the `(start, end]` they cover and their
/// batches, oldest first.
type ViewRows = Vec<ViewWindow>;

/// One window of a view's rows: the `(start, end]` they cover and their
/// batches.
type ViewWindow = ((i64, i64), Arc<Vec<Batch>>);

/// Each materialized view's stored rows. Cloning shares every batch, so a
/// writer copies only the per-view lists when a run still reads them.
#[derive(Default, Clone)]
pub struct ViewStore {
    by_view: BTreeMap<PhysicalASAPNodeId, ViewRows>,
}

impl ViewStore {
    /// Appends `batches` as `view`'s rows covering `time_range_ms`
    /// (`(start, end]`), then drops its rows ending `view.retention_ms()`
    /// or more before `end` (the newest are always kept). Bad input when
    /// the range is empty or starts before the end of the rows appended
    /// last (the store is append-only).
    pub fn append(
        &mut self,
        view: &MaterializedView,
        time_range_ms: (i64, i64),
        batches: Vec<Batch>,
    ) -> Result<(), Error> {
        let (start, end) = time_range_ms;
        let rows = self.by_view.entry(view.node_id).or_default();
        if start >= end
            || rows
                .last()
                .is_some_and(|((_, last_end), _)| start < *last_end)
        {
            return Err(Error::Invalid(format!(
                "materialized view {}: appended rows ({start}, {end}] are empty or not after the stored ones",
                view.node_id
            )));
        }
        rows.push((time_range_ms, Arc::new(batches)));
        let retention_ms = i64::try_from(view.retention_ms()).unwrap_or(i64::MAX);
        let oldest_kept_end = end.saturating_sub(retention_ms);
        rows.retain(|((_, row_end), _)| *row_end > oldest_kept_end || *row_end == end);
        Ok(())
    }

    /// The end of `view`'s newest stored rows.
    pub fn stored_through(&self, view: PhysicalASAPNodeId) -> Option<i64> {
        self.by_view.get(&view)?.last().map(|((_, end), _)| *end)
    }

    /// Bytes of the stored rows.
    pub fn bytes(&self) -> usize {
        self.by_view
            .values()
            .flatten()
            .flat_map(|(_, batches)| batches.iter())
            .map(Batch::memory_bytes)
            .sum()
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
        let selected: Vec<&ViewWindow> = rows
            .iter()
            .filter(|((row_start, row_end), _)| *row_start >= start && *row_end <= end)
            .collect();
        // The selected ranges, oldest first, must tile `(start, end]`.
        let mut covered_to = start;
        for ((row_start, row_end), _) in &selected {
            if *row_start != covered_to {
                return Err(missing());
            }
            covered_to = *row_end;
        }
        if covered_to != end {
            return Err(missing());
        }
        Ok(
            stream::iter(selected.into_iter().flat_map(|(_, b)| b.iter().cloned()))
                .map(move |batch| {
                    if context.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    Ok(batch)
                })
                .boxed_local(),
        )
    }
}

/// The newest published snapshot of the store, which every query run reads.
pub struct PublishedStore {
    snapshot: RwLock<(Arc<RawData>, Arc<ViewStore>)>,
    /// The watermark as of the raw data published, `i64::MIN` before the
    /// first: no sample at or before it is expected any more.
    watermark_ms: std::sync::atomic::AtomicI64,
}

impl PublishedStore {
    pub fn new(raw_data: Arc<RawData>) -> Self {
        Self {
            snapshot: RwLock::new((raw_data, Arc::default())),
            watermark_ms: std::sync::atomic::AtomicI64::new(i64::MIN),
        }
    }

    /// Makes `raw_data`, complete through `watermark_ms`, what new runs
    /// read.
    pub fn publish_raw_data(&self, raw_data: Arc<RawData>, watermark_ms: Option<i64>) {
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
    pub fn snapshot(&self) -> (Arc<RawData>, Arc<ViewStore>) {
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
    ) -> Vec<Vec<Value>> {
        let request = RawDataRequest {
            time_range_ms: range,
            schema,
            predicates: &[],
        };
        raw.read("m_store_test", &request)
            .unwrap()
            .iter()
            .flat_map(Batch::to_rows)
            .collect()
    }

    /// A columnar read returns the rows the row-wise read returned, for
    /// ranges inside one pane, across panes (the open one included) and
    /// unbounded, before and after panes are compacted.
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
            for range in ranges {
                assert_eq!(
                    sorted(read_rows(&raw, &schema, range)),
                    sorted(row_read(&all, &schema, range)),
                    "range {range:?}, sealed {sealed}"
                );
            }
        }
        // A closed pane is compacted into one chunk; the open ones are not.
        let panes = &raw.by_metric["m_store_test"].panes;
        assert_eq!(panes[&5_000].len(), 1);
        assert!(panes[&25_000].len() > 1);
    }

    /// Eviction drops the panes ending at or before the cutoff, keeps the
    /// pane the cutoff falls in, and no read returns a sample at or
    /// before the cutoff.
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
        for range in [None, Some((0, 14_000))] {
            assert_eq!(
                sorted(read_rows(&raw, &schema, range)),
                sorted(row_read(&kept, &schema, range)),
            );
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
