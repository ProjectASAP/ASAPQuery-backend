//! Raw samples to a `RawSampleWindow`'s input batch, column by column.
//!
//! A raw-sample window of the plan (`Scan -> TimeShift* -> TimeRange`)
//! reads samples of one metric. Each series is looked at once: the
//! executor's `RawSampleSelector` applies the `Scan`'s label matchers to
//! it, and its text cells (its series identity and label columns) are
//! computed and cached. A window's batch is then built from the samples'
//! times and values and their series' cached cells, as Arrow columns
//! (`Batch::try_from_columns`): the columns the selector builds, without
//! a `Sample` or a row per sample.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use asap_executor::arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray, UInt32Array,
};
use asap_executor::planner::ir::physical_export::{PhysicalASAPDAG, RawSampleWindow};
use asap_executor::planner::ir::schema::{DataType, FieldDataType};
use asap_executor::planner::ir::{NonASAPOp, Operator, TimeRangeKind};
use asap_executor::values::{Batch, BatchColumn, SchemaRef};
use asap_executor::{series_identity, RawSampleSelector, Sample};

use crate::precompute::RawSample;

/// The `Scan` schema's column that holds a series' full identity.
pub(crate) const SERIES_IDENTITY_COLUMN: &str = "$promql_series_identity";

/// The Prometheus stale marker: a NaN that ends a series, not a sample.
pub(crate) const STALE_MARKER: u64 = 0x7ff0_0000_0000_0002;

/// Whether `value` is the Prometheus stale marker.
pub(crate) fn is_stale(value: f64) -> bool {
    value.to_bits() == STALE_MARKER
}

/// What one column of a window's input holds.
#[derive(Debug, Clone, PartialEq)]
enum ColumnKind {
    Time,
    Value,
    /// The series identity, or a label (empty when the series lacks it).
    Text,
}

/// One selected series' text cells, by input column (`None` for the time
/// and value columns).
#[derive(Debug, PartialEq)]
pub(crate) struct SeriesCells {
    pub(crate) texts: Vec<Option<Arc<str>>>,
}

/// Each series' cells, or `None` when the `Scan` does not select it, with
/// the newest sample time the series was looked up with, by series id.
type SeriesCache = ahash::HashMap<u32, (Option<Arc<SeriesCells>>, AtomicI64)>;

/// One `RawSampleWindow` of the plan, prepared to turn raw samples into its
/// input batch. A series is forgotten once it has not been seen for
/// `keep_ms`, so the cache holds the series of recent windows, not every
/// series ever seen.
pub(crate) struct RawSampleWindowBatcher {
    selector: RawSampleSelector,
    /// The `TimeRange`'s output schema, which the batch is in.
    pub(crate) schema: SchemaRef,
    kinds: Vec<ColumnKind>,
    width_ms: i64,
    /// An instant selector keeps only each series' latest sample.
    latest_sample_per_series: bool,
    series: RwLock<SeriesCache>,
    /// How long a series not seen stays cached, past the window's width.
    keep_ms: i64,
    /// The window end series were last forgotten at.
    forgotten_at_ms: AtomicI64,
}

impl RawSampleWindowBatcher {
    /// The window `window` of `dag`, which caches a series until it has
    /// not been seen for `keep_ms` past the window's width.
    pub(crate) fn new(
        dag: &PhysicalASAPDAG,
        window: &RawSampleWindow,
        keep_ms: i64,
    ) -> Result<Self, asap_executor::Error> {
        let invalid = |message: &str| asap_executor::Error::Invalid(message.into());
        let selector = RawSampleSelector::new(dag, window)?;
        let time_range = dag
            .nodes
            .iter()
            .find(|n| n.id == window.time_range_node_id)
            .ok_or_else(|| invalid("window names an unknown TimeRange"))?;
        let Operator::NonASAP(NonASAPOp::TimeRange { kind, .. }) = &time_range.payload else {
            return Err(invalid("window's time range is not a TimeRange"));
        };
        let schema = Arc::new(time_range.output_schema.clone());
        let kinds = schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if Some(index) == schema.time_index {
                    Ok(ColumnKind::Time)
                } else if field.name == "value"
                    && field.dtype == FieldDataType::Plain(DataType::Float64)
                {
                    Ok(ColumnKind::Value)
                } else if field.dtype == FieldDataType::Plain(DataType::Utf8) {
                    Ok(ColumnKind::Text)
                } else {
                    Err(invalid("unsupported PromQL source column"))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            selector,
            schema,
            kinds,
            width_ms: window.width_ms,
            latest_sample_per_series: *kind == TimeRangeKind::Instant,
            series: RwLock::new(ahash::HashMap::default()),
            keep_ms,
            forgotten_at_ms: AtomicI64::new(i64::MIN),
        })
    }

    /// The metric the window reads.
    pub(crate) fn metric(&self) -> &str {
        self.selector.metric()
    }

    /// The cells of `sample`'s series, or `None` when the `Scan` does not
    /// select it; computed once per series.
    pub(crate) fn cells(&self, sample: &RawSample) -> Option<Arc<SeriesCells>> {
        if let Some((cells, newest_ms)) = self
            .series
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&sample.labels.id)
        {
            newest_ms.fetch_max(sample.timestamp_ms, Ordering::Relaxed);
            return cells.clone();
        }
        let cells = self.series_cells(sample);
        self.series
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                sample.labels.id,
                (cells.clone(), AtomicI64::new(sample.timestamp_ms)),
            );
        cells
    }

    /// How many series are cached.
    #[cfg(test)]
    pub(crate) fn cached_series(&self) -> usize {
        self.series.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Forgets the series whose newest sample is at or before `oldest_ms`.
    pub(crate) fn forget_unseen(&self, oldest_ms: i64) {
        self.forgotten_at_ms.fetch_max(oldest_ms, Ordering::Relaxed);
        self.series
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, (_, newest_ms)| newest_ms.load(Ordering::Relaxed) > oldest_ms);
    }

    /// Forgets the series not seen for `keep_ms` past the width of the
    /// window ending at `end_ms`, at most once per `keep_ms` of window ends,
    /// so a scan of the cache is amortized over many windows.
    fn forget_old(&self, end_ms: i64) {
        let oldest_ms = end_ms
            .saturating_sub(self.width_ms)
            .saturating_sub(self.keep_ms);
        let last = self.forgotten_at_ms.load(Ordering::Relaxed);
        if oldest_ms > last.saturating_add(self.keep_ms.max(1)) {
            self.forget_unseen(oldest_ms);
        }
    }

    fn series_cells(&self, sample: &RawSample) -> Option<Arc<SeriesCells>> {
        // The selector applies the Scan's matchers to the series' labels;
        // the value is not the sample's, which may be a stale marker.
        let one = Sample {
            labels: (**sample.labels).clone(),
            timestamp_ms: sample.timestamp_ms,
            value: 0.0,
        };
        let selected = self
            .selector
            .window_batch(sample.timestamp_ms, &[one])
            .ok()?;
        if selected.num_rows() != 1 {
            return None;
        }
        let identity: Arc<str> = series_identity(&sample.labels).ok()?.into();
        let texts = self
            .kinds
            .iter()
            .zip(&self.schema.fields)
            .map(|(kind, field)| match kind {
                ColumnKind::Text if field.name == SERIES_IDENTITY_COLUMN => Some(identity.clone()),
                ColumnKind::Text => Some(
                    sample
                        .labels
                        .get(&field.name)
                        .map_or_else(|| Arc::from(""), |v| Arc::from(v.as_str())),
                ),
                _ => None,
            })
            .collect();
        Some(Arc::new(SeriesCells { texts }))
    }

    /// The window's input batch for the window ending at `end_ms`, as the
    /// executor's `RawSampleSelector::window_batch` gives it: the samples
    /// in `(end_ms - width, end_ms]` of the series the `Scan` selects
    /// (only each series' latest for an instant selector), in the
    /// `TimeRange`'s schema. A stale marker is never a row: a range window
    /// drops it, and an instant selector whose latest sample is one has no
    /// row for that series.
    pub(crate) fn batch(
        &self,
        end_ms: i64,
        samples: &[RawSample],
    ) -> Result<Batch, asap_executor::Error> {
        let start_ms = end_ms - self.width_ms;
        let in_window = samples
            .iter()
            .filter(|s| start_ms < s.timestamp_ms && s.timestamp_ms <= end_ms);
        let mut points: Vec<(Arc<SeriesCells>, i64, f64)> = Vec::new();
        if self.latest_sample_per_series {
            let mut latest: ahash::HashMap<u32, &RawSample> = ahash::HashMap::default();
            for sample in in_window {
                let entry = latest.entry(sample.labels.id).or_insert(sample);
                if sample.timestamp_ms >= entry.timestamp_ms {
                    *entry = sample;
                }
            }
            // In label order, as the selector emits them.
            let mut latest: Vec<&RawSample> = latest.into_values().collect();
            latest.sort_unstable_by(|a, b| a.labels.cmp(&b.labels));
            for sample in latest.into_iter().filter(|s| !is_stale(s.value)) {
                if let Some(cells) = self.cells(sample) {
                    points.push((cells, sample.timestamp_ms, sample.value));
                }
            }
        } else {
            // Samples of one series are together: look each up once.
            let mut current: Option<(u32, Option<Arc<SeriesCells>>)> = None;
            for sample in in_window.filter(|s| !is_stale(s.value)) {
                let cells = match &current {
                    Some((id, cells)) if *id == sample.labels.id => cells.clone(),
                    _ => {
                        let cells = self.cells(sample);
                        current = Some((sample.labels.id, cells.clone()));
                        cells
                    }
                };
                if let Some(cells) = cells {
                    points.push((cells, sample.timestamp_ms, sample.value));
                }
            }
        }
        self.forget_old(end_ms);
        self.batch_of(&points)
    }

    /// The batch of `points`: each sample's series cells, time and value.
    pub(crate) fn batch_of(
        &self,
        points: &[(Arc<SeriesCells>, i64, f64)],
    ) -> Result<Batch, asap_executor::Error> {
        let columns = self
            .kinds
            .iter()
            .enumerate()
            .map(|(index, kind)| {
                let array: ArrayRef = match kind {
                    ColumnKind::Time => Arc::new(TimestampMillisecondArray::from_iter_values(
                        points.iter().map(|(_, time, _)| *time),
                    )),
                    ColumnKind::Value => Arc::new(Float64Array::from_iter_values(
                        points.iter().map(|(_, _, value)| *value),
                    )),
                    ColumnKind::Text => text_column(
                        points
                            .iter()
                            .map(|(cells, _, _)| cells.texts[index].as_deref().unwrap_or("")),
                    )?,
                };
                Ok(BatchColumn::Plain(array))
            })
            .collect::<Result<Vec<_>, asap_executor::Error>>()?;
        Batch::try_from_columns(self.schema.clone(), columns)
    }
}

/// A text column in the batch layout: a dictionary of the distinct texts,
/// with each row's code.
pub(crate) fn text_column<'a>(
    texts: impl Iterator<Item = &'a str>,
) -> Result<ArrayRef, asap_executor::Error> {
    let mut codes: ahash::HashMap<&str, u32> = ahash::HashMap::default();
    let mut values: Vec<&str> = Vec::new();
    let keys: UInt32Array = texts
        .map(|text| {
            let next = values.len() as u32;
            *codes.entry(text).or_insert_with(|| {
                values.push(text);
                next
            })
        })
        .collect();
    let dictionary = DictionaryArray::try_new(keys, Arc::new(StringArray::from(values)))
        .map_err(|e| asap_executor::Error::Invalid(e.to_string()))?;
    Ok(Arc::new(dictionary))
}
