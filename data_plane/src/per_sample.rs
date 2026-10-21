//! Per-sample ingestion: a definition's build, `Scan -> TimeShift* ->
//! TimeRange(pane) -> SummaryAgg`, run one sample at a time, in columns.
//!
//! - **Per series, once:** the stateless prefix. The window's
//!   [`SeriesWindow`] applies the `Scan`'s label matchers to the series and
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

use std::collections::{BTreeMap, HashMap};
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
use crate::series::{is_stale, text_column, SeriesCells, SERIES_IDENTITY_COLUMN};

/// A group: its texts in the `SummaryAgg`'s group columns, in their order.
type GroupKey = Arc<[Arc<str>]>;

/// Groups numbered `0..`, in the order they are first seen.
#[derive(Default)]
struct GroupNumbers {
    numbers: HashMap<GroupKey, u32>,
    keys: Vec<GroupKey>,
}

impl GroupNumbers {
    fn number(&mut self, key: &GroupKey) -> u32 {
        if let Some(&number) = self.numbers.get(key) {
            return number;
        }
        let number = self.keys.len() as u32;
        self.keys.push(key.clone());
        self.numbers.insert(key.clone(), number);
        number
    }
}

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

/// The open panes of a definition built per sample.
#[derive(Default)]
pub(crate) struct OpenPanes {
    /// The numbers of the groups (series) seen.
    numbers: GroupNumbers,
    /// Each series' group key and number.
    series_groups: HashMap<Arc<BTreeMap<String, String>>, (GroupKey, u32)>,
    /// By pane end: its states and the groups that took a sample, or why it
    /// failed.
    panes: BTreeMap<i64, Result<(StateColumnUpdater, Vec<bool>), String>>,
}

impl OpenPanes {
    /// Drops the open panes ending before `end_ms`, which are skipped.
    pub(crate) fn drop_before(&mut self, end_ms: i64) {
        self.panes = self.panes.split_off(&end_ms);
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

    /// Updates the pane ending at `end_ms`, open in `open`, with a sample
    /// of the series whose cached cells are `cells`.
    pub(crate) fn update(
        &self,
        open: &mut OpenPanes,
        cells: &Arc<SeriesCells>,
        sample: &RawSample,
        end_ms: i64,
    ) {
        // A stale marker ends a series; it is not a sample of its state.
        if is_stale(sample.value) {
            return;
        }
        let group = match open.series_groups.get(&sample.labels) {
            Some(group) => Some(group.clone()),
            None => {
                let key: GroupKey = self
                    .groups
                    .iter()
                    .map(|&c| cells.texts[c].clone().unwrap_or_else(|| Arc::from("")))
                    .collect();
                let number = open.numbers.number(&key);
                open.series_groups
                    .insert(sample.labels.clone(), (key.clone(), number));
                Some((key, number))
            }
        };
        let Some((_, number)) = group else { return };
        let entry = open.panes.entry(end_ms).or_insert_with(|| {
            StateColumnUpdater::new(&self.family)
                .map(|updater| (updater, Vec::new()))
                .map_err(|e| e.to_string())
        });
        let Ok((updater, touched)) = entry else {
            return;
        };
        let index = number as usize;
        let result = updater
            .ensure_groups(index + 1)
            .and_then(|()| updater.update(number, sample.value, sample.timestamp_ms));
        match result {
            Ok(()) => {
                if touched.len() <= index {
                    touched.resize(index + 1, false);
                }
                touched[index] = true;
            }
            Err(error) => *entry = Err(error.to_string()),
        }
    }

    /// The pane ending at `end_ms`, which closes: a row per group (series)
    /// that took a sample.
    pub(crate) fn close(
        &self,
        pane: &SummaryBuild,
        open: &mut OpenPanes,
        end_ms: i64,
        scope: &Scope,
    ) -> Result<Batch, String> {
        let (updater, touched) = match open.panes.remove(&end_ms) {
            Some(open_pane) => open_pane?,
            None => (
                StateColumnUpdater::new(&self.family).map_err(|e| e.to_string())?,
                Vec::new(),
            ),
        };
        let touched: Vec<u32> = touched
            .iter()
            .enumerate()
            .filter(|(_, &took)| took)
            .map(|(number, _)| number as u32)
            .collect();
        self.pane_batch(pane, updater, &touched, &open.numbers.keys, end_ms, scope)
    }

    /// The pane's batch: a row per touched group, in group key order as the
    /// batch build emits them, with its texts, the pane's timestamp and its
    /// state.
    fn pane_batch(
        &self,
        pane: &SummaryBuild,
        updater: StateColumnUpdater,
        touched: &[u32],
        keys: &[GroupKey],
        end_ms: i64,
        scope: &Scope,
    ) -> Result<Batch, String> {
        let mut rows: Vec<(&GroupKey, u32)> = touched
            .iter()
            .map(|&number| (&keys[number as usize], number))
            .collect();
        rows.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let offset = match self.time_offset_ms.get() {
            Some(offset) => *offset,
            None => *self
                .time_offset_ms
                .get_or_init(|| self.calibrate(pane, end_ms, scope).unwrap_or(0)),
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
