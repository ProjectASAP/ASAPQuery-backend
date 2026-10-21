//! Per-sample ingestion: a definition's build, `Scan -> TimeShift* ->
//! TimeRange(pane) -> SummaryAgg`, run one sample at a time, in columns.
//!
//! - **Per series, once:** the stateless prefix. The window's
//!   [`RawSampleWindowBatcher`] applies the `Scan`'s label matchers to the series and
//!   caches its text cells; the cells of the `SummaryAgg`'s group columns
//!   are its group.
//! - **Per sample:** the executor's `StateColumnUpdater` for the open pane
//!   takes the value and time into the group's state in place, the code the
//!   batch `SummaryAgg` runs per row.
//! - **At pane close:** the states are sealed into a state column and the
//!   pane is built from columns (`Batch::try_from_columns`): the group
//!   columns' texts, the pane's timestamp, and the states.
//!
//! A group is one series (the group columns include the series identity),
//! so a counter's samples reach its state in the time order the batch build
//! requires of one series.
//!
//! A build of another shape keeps the batch build over the pane's samples:
//! a row filter, a keyed or shared-grid family, an operator between the
//! `TimeRange` and the `SummaryAgg`, matchers on the sample's value or
//! time, or groups that are not series. The
//! Planner places no such build at ingestion time for the PromQL examples.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use asap_executor::arrow::array::{Array, TimestampMillisecondArray, UInt32Array};
use asap_executor::planner::ir::operator::Reduction;
use asap_executor::planner::ir::physical_export::PhysicalASAPDAG;
use asap_executor::planner::ir::scalar::ColumnRef;
use asap_executor::planner::ir::schema::{
    DataType, FieldDataType, GroupingStrategy, SummaryInputExpr,
};
use asap_executor::planner::ir::{ASAPOp, NonASAPOp, Operator};
use asap_executor::runtime::Scope;
use asap_executor::values::{Batch, BatchColumn, SchemaRef};
use asap_executor::StateColumnUpdater;

use crate::precompute::{RawSample, SummaryBuild};
use crate::raw_sample_window::{
    is_stale, text_column, RawSampleWindowBatcher, SeriesCells, SERIES_IDENTITY_COLUMN,
};

/// A group: its texts in the `SummaryAgg`'s group columns, in their order.
type GroupKey = Arc<[Arc<str>]>;

/// How a definition is built per sample; compiled once per definition.
pub(crate) struct PerSampleSpec {
    family: FieldDataType,
    /// The input group columns, in the `SummaryAgg`'s order.
    groups: Vec<usize>,
    output: SchemaRef,
    /// Output column of the states, and of the pane's timestamp.
    state_column: usize,
    time_column: usize,
    /// Per output text column: its position in a group's key.
    output_group_positions: Vec<(usize, usize)>,
    /// How much the output timestamp is after the pane's end, found by
    /// running the plan's build once.
    time_offset_ms: OnceLock<i64>,
}

/// One series as a per-sample build has seen it.
struct SeriesEntry {
    /// Its group, or `None` when the `Scan` does not select it.
    key: Option<GroupKey>,
    /// Its newest sample time: a series unseen for longer than the
    /// definition's retention is forgotten.
    newest_ms: i64,
    /// The open pane it last took a sample in, and its group number there,
    /// so a run of samples into one pane looks the series up once.
    pane: Option<(i64, u32)>,
}

/// One open pane: a summary state per series that took a sample in it.
struct OpenPane {
    updater: StateColumnUpdater,
    /// Each group's key and newest sample time, by its number in the pane.
    groups: Vec<(GroupKey, i64)>,
    /// Group numbers, by series id.
    numbers: ahash::HashMap<u32, u32>,
}

/// What a per-sample build did with one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Taken {
    /// Taken into its series' state, or not a sample of any (a stale
    /// marker, or a series the `Scan` does not select).
    Taken,
    /// Not after its series' previous sample in the pane (a resent sample,
    /// for instance): dropped, not failing the pane or counted twice.
    Duplicate,
}

/// The open panes of a definition built per sample.
#[derive(Default)]
pub(crate) struct OpenPanes {
    /// Each series seen and not forgotten, by series id.
    series: ahash::HashMap<u32, SeriesEntry>,
    /// By pane end: its states, or why it failed.
    panes: BTreeMap<i64, Result<OpenPane, String>>,
}

impl OpenPanes {
    /// Drops the open panes ending before `end_ms`, which are skipped.
    pub(crate) fn drop_before(&mut self, end_ms: i64) {
        self.panes = self.panes.split_off(&end_ms);
    }

    /// The series remembered, and each open pane's number of states.
    #[cfg(test)]
    pub(crate) fn sizes(&self) -> (usize, Vec<usize>) {
        let states = self
            .panes
            .values()
            .map(|pane| pane.as_ref().map_or(0, |p| p.updater.group_count()))
            .collect();
        (self.series.len(), states)
    }
}

impl PerSampleSpec {
    /// The per-sample build of `pane`'s definition, or `None` when its shape
    /// keeps the batch build.
    pub(crate) fn new(dag: &PhysicalASAPDAG, pane: &SummaryBuild) -> Option<Self> {
        let definition = &pane.definition;
        let node = |id: u64| dag.nodes.iter().find(|n| n.id as u64 == id);
        let builder = node(definition.builder)?;
        let inputs: Vec<u64> = dag
            .edges
            .iter()
            .filter(|e| e.consumer as u64 == definition.builder)
            .map(|e| e.producer as u64)
            .collect();
        if inputs != [definition.raw_input()] {
            return None;
        }
        let Operator::ASAP(ASAPOp::SummaryAgg {
            family,
            input: update,
            reduction,
            grouping,
            filter: None,
            ..
        }) = &builder.payload
        else {
            return None;
        };
        if update.item.is_some() || *grouping != GroupingStrategy::PerSubpopulationInstance {
            return None;
        }
        let input = &node(definition.raw_input())?.output_schema;
        let SummaryInputExpr::Column(column) = &update.weight else {
            return None;
        };
        let name = match column {
            ColumnRef::SampleValue => "value",
            ColumnRef::Named(name) => name.as_str(),
            _ => return None,
        };
        let value = input.fields.iter().position(|f| f.name == name)?;
        let time = input.time_index?;
        let groups: Vec<usize> = match reduction {
            Reduction::PerEntity => (0..input.fields.len())
                .filter(|&c| c != value && c != time)
                .collect(),
            Reduction::Reduce(keys) if !keys.is_without() && !keys.keys().is_empty() => {
                keys.keys().to_vec()
            }
            _ => return None,
        };
        if groups
            .iter()
            .any(|&c| input.fields[c].dtype != FieldDataType::Plain(DataType::Utf8))
        {
            return None;
        }
        // The cached prefix holds only if the matchers read labels alone.
        let scan = node(definition.window.scan_node_id as u64)?;
        let Operator::NonASAP(NonASAPOp::Scan { predicates, .. }) = &scan.payload else {
            return None;
        };
        let read = serde_json::to_value(predicates).ok()?;
        if columns_read(&read).any(|c| c == value || c == time) {
            return None;
        }
        StateColumnUpdater::new(family).ok()?;
        let output = Arc::new(definition.output_schema.clone());
        let state_column = output
            .fields
            .iter()
            .position(|f| !matches!(f.dtype, FieldDataType::Plain(_)))?;
        let time_column = output.time_index?;
        // Every other output column repeats a group column of its name.
        let output_group_positions = output
            .fields
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != state_column && *i != time_column)
            .map(|(i, field)| {
                let position = groups
                    .iter()
                    .position(|&c| input.fields[c].name == field.name)?;
                (field.dtype == FieldDataType::Plain(DataType::Utf8)).then_some((i, position))
            })
            .collect::<Option<Vec<_>>>()?;
        // A group is one series.
        if !groups
            .iter()
            .any(|&c| input.fields[c].name == SERIES_IDENTITY_COLUMN)
        {
            return None;
        }
        Some(Self {
            family: family.clone(),
            groups,
            output,
            state_column,
            time_column,
            output_group_positions,
            time_offset_ms: OnceLock::new(),
        })
    }

    /// Takes `sample` into the pane ending at `end_ms`, open in `open`:
    /// its series' summary state there is updated in place. `window` gives
    /// a series' group the first time it is seen.
    pub(crate) fn update(
        &self,
        window: &RawSampleWindowBatcher,
        open: &mut OpenPanes,
        sample: &RawSample,
        end_ms: i64,
    ) -> Taken {
        // A stale marker ends a series; it is not a sample of its state.
        if is_stale(sample.value) {
            return Taken::Taken;
        }
        let id = sample.labels.id;
        let entry = open.series.entry(id).or_insert_with(|| SeriesEntry {
            key: window.cells(sample).map(|cells| {
                self.groups
                    .iter()
                    .map(|&c| cells.texts[c].clone().unwrap_or_else(|| Arc::from("")))
                    .collect()
            }),
            newest_ms: sample.timestamp_ms,
            pane: None,
        });
        entry.newest_ms = entry.newest_ms.max(sample.timestamp_ms);
        let Some(key) = &entry.key else {
            return Taken::Taken;
        };
        let pane = open.panes.entry(end_ms).or_insert_with(|| {
            StateColumnUpdater::new(&self.family)
                .map(|updater| OpenPane {
                    updater,
                    groups: Vec::new(),
                    numbers: ahash::HashMap::default(),
                })
                .map_err(|e| e.to_string())
        });
        let Ok(pane) = pane else {
            return Taken::Taken;
        };
        let number = match entry.pane {
            Some((end, number)) if end == end_ms => Ok(number),
            _ => match pane.numbers.get(&id) {
                Some(&number) => Ok(number),
                None => pane.updater.add_group().inspect(|&number| {
                    pane.numbers.insert(id, number);
                    pane.groups.push((key.clone(), i64::MIN));
                }),
            },
        };
        let result = number.and_then(|number| {
            entry.pane = Some((end_ms, number));
            let newest = &mut pane.groups[number as usize].1;
            // As Prometheus does, a series' sample not after its previous one
            // (a resent sample, for instance) is dropped: a counter's state
            // refuses it, and any other state would count it twice.
            if sample.timestamp_ms <= *newest {
                return Ok(Taken::Duplicate);
            }
            *newest = sample.timestamp_ms;
            pane.updater
                .update(number, sample.value, sample.timestamp_ms)
                .map(|()| Taken::Taken)
        });
        match result {
            Ok(taken) => taken,
            Err(error) => {
                open.panes.insert(end_ms, Err(error.to_string()));
                Taken::Taken
            }
        }
    }

    /// The pane ending at `end_ms`, which closes: a row per group (series)
    /// that took a sample. Then the series not seen for longer than
    /// `keep_ms` before the pane's end are forgotten.
    pub(crate) fn close(
        &self,
        build: &SummaryBuild,
        open: &mut OpenPanes,
        end_ms: i64,
        keep_ms: i64,
        scope: &Scope,
    ) -> Result<Batch, String> {
        let oldest_ms = end_ms.saturating_sub(keep_ms);
        open.series.retain(|_, entry| entry.newest_ms > oldest_ms);
        build.window.forget_unseen(oldest_ms);
        let (updater, groups) = match open.panes.remove(&end_ms) {
            Some(pane) => {
                let pane = pane?;
                (pane.updater, pane.groups)
            }
            None => (
                StateColumnUpdater::new(&self.family).map_err(|e| e.to_string())?,
                Vec::new(),
            ),
        };
        self.pane_batch(build, updater, &groups, end_ms, scope)
    }

    /// The pane's batch: a row per group, in group key order as the batch
    /// build emits them, with its texts, the pane's timestamp and its state.
    fn pane_batch(
        &self,
        build: &SummaryBuild,
        updater: StateColumnUpdater,
        groups: &[(GroupKey, i64)],
        end_ms: i64,
        scope: &Scope,
    ) -> Result<Batch, String> {
        let mut rows: Vec<(&GroupKey, u32)> = groups
            .iter()
            .enumerate()
            .map(|(number, (key, _))| (key, number as u32))
            .collect();
        rows.sort_unstable_by(|a, b| a.0.cmp(b.0));
        // Found once; a pane fails, and the next one tries again, until the
        // build gives a timestamp.
        let offset = match self.time_offset_ms.get() {
            Some(offset) => *offset,
            None => {
                let offset = self.calibrate(build, end_ms, scope)?;
                *self.time_offset_ms.get_or_init(|| offset)
            }
        };
        let states = Arc::new(updater.seal());
        let columns = (0..self.output.fields.len())
            .map(|column| {
                if column == self.state_column {
                    return Ok(BatchColumn::States {
                        indices: rows
                            .iter()
                            .map(|(_, number)| *number)
                            .collect::<UInt32Array>(),
                        states: states.clone(),
                    });
                }
                if column == self.time_column {
                    return Ok(BatchColumn::Plain(Arc::new(
                        TimestampMillisecondArray::from_iter_values(
                            rows.iter().map(|_| end_ms + offset),
                        ),
                    )));
                }
                let (_, position) = self
                    .output_group_positions
                    .iter()
                    .find(|(output, _)| *output == column)
                    .expect("every other column repeats a group column");
                text_column(rows.iter().map(|(key, _)| key[*position].as_ref()))
                    .map(BatchColumn::Plain)
            })
            .collect::<Result<Vec<_>, asap_executor::Error>>()
            .map_err(|e| e.to_string())?;
        Batch::try_from_columns(self.output.clone(), columns).map_err(|e| e.to_string())
    }

    /// How much the build's output timestamp is after the pane's end: the
    /// plan's build run once over one row.
    fn calibrate(&self, pane: &SummaryBuild, end_ms: i64, scope: &Scope) -> Result<i64, String> {
        let texts = pane
            .window
            .schema
            .fields
            .iter()
            .map(|field| field.dtype == FieldDataType::Plain(DataType::Utf8))
            .map(|text| text.then(|| Arc::from("")))
            .collect();
        let cells = Arc::new(SeriesCells { texts });
        let row = pane
            .window
            .batch_of(&[(cells, end_ms, 1.0)])
            .map_err(|e| e.to_string())?;
        let built = pane
            .run_batch(row, scope.clone())
            .map_err(|e| e.to_string())?;
        let time = built
            .batches
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .and_then(|batch| {
                batch
                    .column(self.time_column)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .filter(|times| !times.is_null(0))
                    .map(|times| times.value(0))
            })
            .ok_or("the build gave no timestamp")?;
        Ok(time - end_ms)
    }
}

/// The column indices an expression's JSON form reads (`{"Column": i}`).
fn columns_read(expression: &serde_json::Value) -> Box<dyn Iterator<Item = usize> + '_> {
    match expression {
        serde_json::Value::Object(fields) => Box::new(fields.iter().flat_map(|(name, value)| {
            let column = match (name.as_str(), value) {
                ("Column", serde_json::Value::Number(n)) => n.as_u64().map(|c| c as usize),
                _ => None,
            };
            column.into_iter().chain(columns_read(value))
        })),
        serde_json::Value::Array(items) => Box::new(items.iter().flat_map(columns_read)),
        _ => Box::new(std::iter::empty()),
    }
}
