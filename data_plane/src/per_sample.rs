//! Per-sample ingestion: a definition's build, `Scan -> TimeShift* ->
//! TimeRange(pane) -> SummaryAgg`, run one sample at a time instead of
//! over a pane's buffered rows.
//!
//! - **Per series, once:** the stateless prefix. The executor's
//!   `RawSampleSelector` turns the series' first sample into its input row,
//!   which applies the `Scan`'s label matchers; the row's group columns give
//!   its group. The result is cached by label set.
//! - **Per sample:** the value updates its open pane's state for its group
//!   in place, through the updater the executor's batch build uses for the
//!   family (`asap_executor::factory::create_planner_accumulator`), with the
//!   same value and timestamp, in the same order.
//! - **Per group, once:** the output row's layout. The definition's compiled
//!   build runs over the group's first row, and its output row is kept as
//!   the group's template; at pane close each group's row is its template
//!   with the pane's timestamp and the group's state.
//!
//! A build of another shape (a row filter, a keyed or shared-grid family,
//! an operator between the `TimeRange` and the `SummaryAgg`, matchers on the
//! sample's value or time) keeps the batch build.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use asap_executor::factory::{create_planner_accumulator, AccumulatorUpdater};
use asap_executor::planner::ir::operator::Reduction;
use asap_executor::planner::ir::scalar::ColumnRef;
use asap_executor::planner::ir::schema::{
    ExactKind, FieldDataType, GroupingStrategy, SummaryInputExpr, SummaryUpdate,
};
use asap_executor::planner::ir::{ASAPOp, NonASAPOp, Operator};
use asap_executor::runtime::Scope;
use asap_executor::values::{Batch, Value};
use asap_planner::PhysicalASAPDAG;

use crate::precompute::{PaneBuild, RawSample};

type Labels = Arc<BTreeMap<String, String>>;

/// One group of a definition: its key and what its output rows repeat.
struct Group {
    /// The group's first input row, for its template.
    row: Batch,
    /// Its output row, with the timestamp and state to be replaced; built
    /// when the group's first pane closes.
    template: Option<Vec<Value>>,
}

/// One group's state in one open pane.
struct OpenState {
    updater: Box<dyn AccumulatorUpdater>,
    /// The previous sample's time, for counters, which need increasing times.
    previous_ms: Option<i64>,
}

/// One open pane: its groups' states, by group key, or why it failed.
type OpenPane = Result<BTreeMap<Vec<Vec<u8>>, OpenState>, String>;

/// A definition's build, run per sample.
pub(crate) struct PerSampleBuild {
    family: FieldDataType,
    update: SummaryUpdate,
    grouping: GroupingStrategy,
    /// Input columns: the value, the time, the group key.
    value: usize,
    time: usize,
    groups: Vec<usize>,
    /// Counters (rate, increase) need strictly increasing times in a group.
    ordered_time: bool,
    /// Output columns of the state and the timestamp.
    state_column: usize,
    time_column: usize,
    /// Each series' group key, or `None` if the `Scan` does not select it.
    series: HashMap<Labels, Option<Vec<Vec<u8>>>>,
    group_rows: HashMap<Vec<Vec<u8>>, Group>,
    /// How much the output timestamp is after the pane's end.
    time_offset_ms: Option<i64>,
    open: BTreeMap<i64, OpenPane>,
}

impl PerSampleBuild {
    /// The per-sample build of `pane`'s definition, or `None` when its shape
    /// keeps the batch build.
    pub(crate) fn new(dag: &PhysicalASAPDAG, pane: &PaneBuild) -> Option<Self> {
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
        // The cached prefix holds only if the matchers read labels alone.
        let scan = node(definition.window.scan_node_id as u64)?;
        let Operator::NonASAP(NonASAPOp::Scan { predicates, .. }) = &scan.payload else {
            return None;
        };
        let read = serde_json::to_value(predicates).ok()?;
        if columns_read(&read).any(|c| c == value || c == time) {
            return None;
        }
        create_planner_accumulator(family, update, grouping).ok()?;
        let output = &definition.output_schema;
        let state_column = output
            .fields
            .iter()
            .position(|f| !matches!(f.dtype, FieldDataType::Plain(_)))?;
        let time_column = output.time_index?;
        let ordered_time = matches!(
            family,
            FieldDataType::ExactAggregate(ExactKind::Rate | ExactKind::Increase, _)
        );
        Some(Self {
            family: family.clone(),
            update: update.clone(),
            grouping: grouping.clone(),
            value,
            time,
            groups,
            ordered_time,
            state_column,
            time_column,
            series: HashMap::new(),
            group_rows: HashMap::new(),
            time_offset_ms: None,
            open: BTreeMap::new(),
        })
    }

    /// Updates the pane ending at `end_ms` with `sample`.
    pub(crate) fn update(&mut self, pane: &PaneBuild, sample: &RawSample, end_ms: i64) {
        let key = match self.series.get(&sample.labels) {
            Some(key) => key.clone(),
            None => {
                let key = self.series_group(pane, sample);
                self.series.insert(sample.labels.clone(), key.clone());
                key
            }
        };
        let Some(key) = key else { return };
        let Ok(states) = self
            .open
            .entry(end_ms)
            .or_insert_with(|| Ok(BTreeMap::new()))
        else {
            return;
        };
        let state = match states.get_mut(&key) {
            Some(state) => state,
            None => {
                let updater =
                    create_planner_accumulator(&self.family, &self.update, &self.grouping)
                        .expect("checked at install");
                states.entry(key).or_insert(OpenState {
                    updater,
                    previous_ms: None,
                })
            }
        };
        let time = sample.timestamp_ms;
        let result = if self.ordered_time && state.previous_ms.is_some_and(|p| time <= p) {
            Err("counter samples must have strictly increasing timestamps within each group".into())
        } else {
            state.previous_ms = Some(time);
            state
                .updater
                .update_value(&Value::Float64(sample.value), time)
        };
        if let Err(error) = result {
            self.open.insert(end_ms, Err(error));
        }
    }

    /// The group of `sample`'s series: its input row's group columns, if
    /// the `Scan` selects it.
    fn series_group(&mut self, pane: &PaneBuild, sample: &RawSample) -> Option<Vec<Vec<u8>>> {
        let row = pane
            .selector
            .rows(sample.timestamp_ms, &[sample.to_sample()])
            .ok()?;
        let cells = row.rows().first()?;
        debug_assert!(matches!(cells[self.value], Value::Float64(_)));
        debug_assert!(matches!(cells[self.time], Value::Timestamp(_)));
        let key = self
            .groups
            .iter()
            .map(|&c| cells[c].key())
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        self.group_rows.entry(key.clone()).or_insert(Group {
            row,
            template: None,
        });
        Some(key)
    }

    /// The pane ending at `end_ms`, which closes: each group's row, in group
    /// key order as the batch build emits them.
    pub(crate) fn close(
        &mut self,
        pane: &PaneBuild,
        end_ms: i64,
        scope: Scope,
    ) -> Result<Batch, String> {
        let states = self
            .open
            .remove(&end_ms)
            .unwrap_or_else(|| Ok(BTreeMap::new()))?;
        let mut rows = Vec::with_capacity(states.len());
        for (key, state) in states {
            let group = self.group_rows.get_mut(&key).expect("grouped series");
            if group.template.is_none() {
                let built = pane
                    .run_rows(group.row.clone(), scope.clone())
                    .map_err(|e| e.to_string())?;
                let [row] = built.rows() else {
                    return Err("a group's build did not give one row".into());
                };
                let Value::Timestamp(time) = row[self.time_column] else {
                    return Err("output time column is not a timestamp".into());
                };
                let offset = time - end_ms;
                if *self.time_offset_ms.get_or_insert(offset) != offset {
                    return Err("output timestamps differ between groups".into());
                }
                group.template = Some(row.clone());
            }
            let mut row = group.template.clone().expect("built above");
            row[self.time_column] =
                Value::Timestamp(end_ms + self.time_offset_ms.expect("set with the template"));
            row[self.state_column] = Value::Summary {
                family: self.family.clone(),
                state: Arc::from(state.updater.into_accumulator()),
            };
            rows.push(row);
        }
        let schema = Arc::new(pane.definition.output_schema.clone());
        Batch::try_new(schema, rows).map_err(|e| e.to_string())
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
