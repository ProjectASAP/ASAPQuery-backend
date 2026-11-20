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
//! A run reads a [`RunStore`]: an immutable snapshot sharing both parts.
//! The writer changes its own copy (copy on write: a part a run still
//! reads is cloned first) and publishes it ([`PublishedStore`]), so a run
//! never sees a change and never blocks one.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use asap_executor::arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray, UInt32Array,
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

use crate::series_ids::Series;

/// Rows per raw-data batch a read returns.
const RAW_BATCH_ROWS: usize = 64 * 1024;

/// One raw sample: its series (interned: its id and full label set,
/// `__name__` included), time and value.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    pub labels: Arc<Series>,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// The raw samples of each metric the plan scans, in arrival order.
#[derive(Debug, Default, Clone)]
pub struct RawData {
    by_metric: HashMap<String, Vec<RawSample>>,
}

impl RawData {
    /// Keeps the samples of `metrics` from now on; samples of any other
    /// metric are not stored ([`Self::reads`]).
    pub fn for_metrics<'a>(metrics: impl IntoIterator<Item = &'a String>) -> Self {
        Self {
            by_metric: metrics
                .into_iter()
                .map(|metric| (metric.clone(), Vec::new()))
                .collect(),
        }
    }

    /// Whether some `Scan` reads `metric`.
    pub fn reads(&self, metric: &str) -> bool {
        self.by_metric.contains_key(metric)
    }

    /// Appends `sample`; `false` (not stored) when no `Scan` reads its metric.
    pub fn push(&mut self, sample: RawSample) -> bool {
        match self.by_metric.get_mut(&*sample.labels.metric) {
            Some(samples) => {
                samples.push(sample);
                true
            }
            None => false,
        }
    }

    /// Drops `metric`'s samples at or before `oldest_kept_ms`.
    pub fn evict_through(&mut self, metric: &str, oldest_kept_ms: i64) {
        if let Some(samples) = self.by_metric.get_mut(metric) {
            if samples.iter().any(|s| s.timestamp_ms <= oldest_kept_ms) {
                samples.retain(|s| s.timestamp_ms > oldest_kept_ms);
            }
        }
    }

    /// Samples held, over every metric.
    pub fn len(&self) -> usize {
        self.by_metric.values().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `metric`'s samples in `time_range_ms` (`(start, end]`, every sample
    /// when `None`) as batches of `request`'s schema.
    fn read(&self, metric: &str, request: &RawDataRequest) -> Result<Vec<Batch>, Error> {
        let samples = self.by_metric.get(metric).map(Vec::as_slice).unwrap_or(&[]);
        let in_range: Vec<&RawSample> = match request.time_range_ms {
            Some((start, end)) => samples
                .iter()
                .filter(|s| start < s.timestamp_ms && s.timestamp_ms <= end)
                .collect(),
            None => samples.iter().collect(),
        };
        let columns: Vec<ColumnKind> = request
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
            .collect::<Result<_, _>>()?;
        in_range
            .chunks(RAW_BATCH_ROWS)
            .map(|rows| {
                let columns = columns
                    .iter()
                    .map(|kind| kind.column(rows).map(BatchColumn::Plain))
                    .collect::<Result<Vec<_>, Error>>()?;
                Batch::try_from_columns(request.schema.clone(), columns)
            })
            .collect()
    }
}

/// What one column of a time series table holds.
enum ColumnKind {
    Time,
    Value,
    /// The canonical JSON of every label, `__name__` included.
    SeriesIdentity,
    /// One label's value, empty when the series lacks it.
    Label(String),
}

impl ColumnKind {
    fn column(&self, rows: &[&RawSample]) -> Result<ArrayRef, Error> {
        Ok(match self {
            Self::Time => Arc::new(TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|s| s.timestamp_ms),
            )),
            Self::Value => Arc::new(Float64Array::from_iter_values(rows.iter().map(|s| s.value))),
            Self::SeriesIdentity => series_text_column(rows, |series| {
                serde_json::to_string(&**series).expect("labels serialize")
            })?,
            Self::Label(name) => {
                series_text_column(rows, |series| series.get(name).cloned().unwrap_or_default())?
            }
        })
    }
}

/// A text column computed once per series of `rows`, as the batch layout
/// holds text: a dictionary of the distinct texts and each row's code.
fn series_text_column(
    rows: &[&RawSample],
    text_of: impl Fn(&Series) -> String,
) -> Result<ArrayRef, Error> {
    let mut code_of_series: HashMap<u32, u32> = HashMap::new();
    let mut code_of_text: HashMap<String, u32> = HashMap::new();
    let mut texts: Vec<String> = Vec::new();
    let keys: UInt32Array = rows
        .iter()
        .map(|sample| {
            *code_of_series.entry(sample.labels.id).or_insert_with(|| {
                let text = text_of(&sample.labels);
                *code_of_text.entry(text.clone()).or_insert_with(|| {
                    texts.push(text);
                    texts.len() as u32 - 1
                })
            })
        })
        .collect();
    let dictionary = DictionaryArray::try_new(keys, Arc::new(StringArray::from(texts)))
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(Arc::new(dictionary))
}

/// What one run reads: the store as it was when the run started. It
/// records the first view read that found no rows, which says the query
/// cannot be answered from what is stored (rather than that it failed).
pub struct RunStore {
    raw_data: Arc<RawData>,
    views: Arc<ViewStore>,
    missing_view_rows: Mutex<Option<String>>,
}

impl RunStore {
    pub fn new(raw_data: Arc<RawData>, views: Arc<ViewStore>) -> Self {
        Self {
            raw_data,
            views,
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

/// The working copy of the store, owned by its writer: raw data and
/// materialized views, each shared with the snapshots already published
/// and cloned before it changes while one still reads it.
#[derive(Default)]
pub struct TimeIndexedStore {
    raw_data: Arc<RawData>,
    views: Arc<ViewStore>,
}

impl TimeIndexedStore {
    /// An empty store keeping the samples of `metrics`.
    pub fn for_metrics<'a>(metrics: impl IntoIterator<Item = &'a String>) -> Self {
        Self {
            raw_data: Arc::new(RawData::for_metrics(metrics)),
            views: Arc::default(),
        }
    }

    /// The snapshot one run reads.
    pub fn run_store(&self) -> Arc<RunStore> {
        Arc::new(RunStore::new(self.raw_data.clone(), self.views.clone()))
    }

    pub fn raw_data(&self) -> &RawData {
        &self.raw_data
    }

    /// The raw data, to append or evict.
    pub fn raw_data_mut(&mut self) -> &mut RawData {
        Arc::make_mut(&mut self.raw_data)
    }

    /// Appends `batches` as `view`'s rows covering `time_range_ms`.
    pub fn append_view_rows(
        &mut self,
        view: &MaterializedView,
        time_range_ms: (i64, i64),
        batches: Vec<Batch>,
    ) -> Result<(), Error> {
        Arc::make_mut(&mut self.views).append(view, time_range_ms, batches)
    }
}

/// The newest published snapshot of the store, which every query run reads.
#[derive(Default)]
pub struct PublishedStore {
    snapshot: RwLock<(Arc<RawData>, Arc<ViewStore>)>,
}

impl PublishedStore {
    /// Makes `store` as it is now what new runs read.
    pub fn publish(&self, store: &TimeIndexedStore) {
        *self.snapshot.write().unwrap_or_else(|e| e.into_inner()) =
            (store.raw_data.clone(), store.views.clone());
    }

    /// The snapshot one run reads: the newest published.
    pub fn run_store(&self) -> Arc<RunStore> {
        let (raw_data, views) = self
            .snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        Arc::new(RunStore::new(raw_data, views))
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
