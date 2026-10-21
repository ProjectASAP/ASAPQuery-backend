//! Stored summaries (SDS): the summary instances the data plane stores for a
//! plan, and how a query finds them again.
//!
//! The installed plan lists every output it stores across evaluations
//! (`InstallablePlan::stored_outputs`): which run builds it (an
//! `IngestionRun`, or the `QueryRun` of the first evaluation that reads it),
//! its `SummaryCoverage` (the plan's definition id and selection), the
//! `RawSampleWindow`s and stored outputs its build reads, and its
//! `retention_ms`.
//!
//! A definition's identity is the plan's definition id. Outputs with the same
//! definition id, the same label constraints in their selection and the
//! same pane width differ only in where their time interval ends, so they
//! share one [`SummaryDefinition`]: the data plane builds each pane once and
//! every output reads the one it needs. Outputs with the same definition id
//! but other label constraints or another pane width (e.g. one query's
//! 1-second panes and another's 30-second panes of the same per-series sum)
//! are other [`SummaryDefinition`]s ([`DefinitionKey`]). A summary instance
//! is keyed by its definition id and its concrete [`Selection`]: those label
//! constraints and the time interval `(end - width, end]` the plan's
//! relative time binds to at evaluation time.
//!
//! A stored output may be built from other stored outputs (rollups over time
//! or across series). [`Sds::ingestion_build_order`] orders definitions so
//! that each one's stored inputs come first; the precompute engine refuses
//! such a definition until it can build it once all its stored inputs are
//! sealed.

use std::collections::{BTreeMap, HashMap};

use std::ops::Bound;
use std::sync::Arc;

use asap_executor::planner::ir::physical_export::{PhysicalASAPDAGNode, RawSampleWindow};
use asap_executor::planner::ir::properties::summary_coverage::SelectionBox;
use asap_executor::planner::ir::schema::Schema;
use asap_planner::{InstallablePlan, StoredOutputBuild};

/// What a stored summary computes: the plan's
/// `StoredCoverage::definition_id`, equal on every node, plan and process
/// that computes the same thing, whatever its selection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionId(pub String);

/// The label constraints of a selection (its `SelectionBox` columns), in the
/// plan's JSON form; empty when it has none.
pub type LabelConstraints = Arc<str>;

/// One [`SummaryDefinition`] of the SDS: a definition id, and the label
/// constraints and pane width its outputs share.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionKey {
    pub id: DefinitionId,
    pub labels: LabelConstraints,
    pub pane_ms: i64,
}

impl DefinitionKey {
    /// `id`'s panes `pane_ms` wide with no label constraints.
    pub fn unconstrained(id: DefinitionId, pane_ms: i64) -> Self {
        Self {
            id,
            labels: Arc::from(""),
            pane_ms,
        }
    }
}

/// One computation the data plane stores pane by pane, for one set of label
/// constraints.
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryDefinition {
    pub id: DefinitionId,
    /// The label constraints of its outputs' selection.
    pub labels: LabelConstraints,
    /// Which run builds its stored outputs (`StoredOutputBuild`): an
    /// `IngestionRun`, or the `QueryRun` of the first evaluation that needs
    /// one (stored once its coverage is closed). `IngestionRun` when any
    /// output of the definition says so.
    pub built_at: StoredOutputBuild,
    /// Pane width: each summary instance covers `(end - pane_ms, end]`.
    pub pane_ms: i64,
    /// The output that builds a pane; any output of the definition computes
    /// the same, and this is the first in node order.
    pub builder: u64,
    /// The `RawSampleWindow` the builder reads: its `TimeRange`, fed one
    /// pane's raw samples, and the `Scan` that selects them. `None` for a
    /// definition built from other stored outputs only.
    pub window: Option<RawSampleWindow>,
    pub output_schema: Schema,
    /// The definitions of the stored outputs the builder reads, which must
    /// be built first. Empty while every stored output reads raw samples.
    pub stored_inputs: Vec<DefinitionKey>,
    /// How far back from the newest evaluation time a summary stays available:
    /// the largest `retention_ms` the plan states for the definition's
    /// outputs. A pane that starts earlier is no longer read.
    pub retention_ms: i64,
}

impl SummaryDefinition {
    /// Its key in [`Sds::definitions`].
    pub fn key(&self) -> DefinitionKey {
        DefinitionKey {
            id: self.id.clone(),
            labels: self.labels.clone(),
            pane_ms: self.pane_ms,
        }
    }

    /// The concrete selection of its pane ending at `end_ms`.
    pub fn selection_ending_at(&self, end_ms: i64) -> Selection {
        self.key().selection_ending_at(end_ms)
    }

    /// The builder's `TimeRange`, which one pane's raw samples feed.
    pub fn raw_input(&self) -> Option<u64> {
        self.window.as_ref().map(|w| w.time_range_node_id as u64)
    }

    /// The end of the oldest pane still read when the newest evaluation is
    /// at `newest_ms`: panes ending before it are evicted.
    pub fn oldest_kept_end_ms(&self, newest_ms: i64) -> i64 {
        newest_ms
            .saturating_sub(self.retention_ms)
            .saturating_add(self.pane_ms)
    }
}

/// How one output reads its definition: at evaluation time `T`, the pane
/// ending at `T - offset_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaneBinding {
    pub offset_ms: i64,
    /// The output's own `TimeRange` node: where its coverage's raw samples
    /// feed when a stored output built in a `QueryRun` is built for it.
    pub raw_input: u64,
}

/// The stored summaries of one plan.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Sds {
    /// Every definition, built in an `IngestionRun` or a `QueryRun`.
    pub definitions: BTreeMap<DefinitionKey, SummaryDefinition>,
    /// Every stored output, with its definition and pane.
    pub outputs: BTreeMap<u64, (DefinitionKey, PaneBinding)>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SdsError {
    #[error("node {0} is not in the DAG")]
    UnknownNode(u64),
    #[error("stored output {0}'s retention does not fit in i64 milliseconds")]
    RetentionOverflow(u64),
    /// Stored outputs read each other in a cycle.
    #[error("stored outputs read each other in a cycle through {0:?}")]
    Cycle(DefinitionKey),
    /// An `IngestionRun` definition reads a `QueryRun` one, which is built
    /// only when an evaluation needs it, so never in time for ingestion.
    #[error("stored output built in an IngestionRun ({definition:?}) reads one built in a QueryRun ({input:?})")]
    IngestionReadsQueryRun {
        definition: DefinitionKey,
        input: DefinitionKey,
    },
    /// A stored output's coverage or inputs are not what the data plane can
    /// build.
    #[error("stored output {node}: {reason}")]
    Unsupported { node: u64, reason: String },
}

impl Sds {
    /// The definitions behind `plan`'s stored outputs, built in an
    /// `IngestionRun` or in a `QueryRun`.
    pub fn from_plan(plan: &InstallablePlan) -> Result<Self, SdsError> {
        let dag = &plan.dag.dag;
        let nodes: HashMap<u64, &PhysicalASAPDAGNode> =
            dag.nodes.iter().map(|n| (n.id as u64, n)).collect();
        let mut sds = Sds::default();
        let mut stored_inputs: HashMap<u64, Vec<u64>> = HashMap::new();
        for output in &plan.stored_outputs {
            let node = output.node_id as u64;
            let unsupported = |reason: &str| SdsError::Unsupported {
                node,
                reason: reason.into(),
            };
            let output_schema = nodes
                .get(&node)
                .ok_or(SdsError::UnknownNode(node))?
                .output_schema
                .clone();
            let [selection] = output.coverage.selection.as_slice() else {
                return Err(unsupported("its selection is not one box"));
            };
            let (start, end) = relative_time(selection)
                .ok_or_else(|| unsupported("its selection has no left-open time interval"))?;
            let window = match output.inputs.raw_sample_windows.as_slice() {
                [] => None,
                [window] => Some(window.clone()),
                _ => return Err(unsupported("it reads several raw-sample windows")),
            };
            if window.is_none() && output.inputs.stored_outputs.is_empty() {
                return Err(unsupported("it reads no input"));
            }
            let key = DefinitionKey {
                id: DefinitionId(output.coverage.definition_id.clone()),
                labels: label_constraints(selection),
                pane_ms: end - start,
            };
            let retention_ms = i64::try_from(output.retention_ms)
                .map_err(|_| SdsError::RetentionOverflow(node))?;
            let definition =
                sds.definitions
                    .entry(key.clone())
                    .or_insert_with(|| SummaryDefinition {
                        id: key.id.clone(),
                        labels: key.labels.clone(),
                        built_at: output.built_at,
                        pane_ms: key.pane_ms,
                        builder: node,
                        window: window.clone(),
                        output_schema,
                        stored_inputs: Vec::new(),
                        retention_ms,
                    });
            if output.built_at == StoredOutputBuild::IngestionRun {
                definition.built_at = StoredOutputBuild::IngestionRun;
            }
            if node < definition.builder {
                definition.builder = node;
                definition.window = window.clone();
            }
            definition.retention_ms = definition.retention_ms.max(retention_ms);
            let binding = PaneBinding {
                offset_ms: -end,
                raw_input: window
                    .as_ref()
                    .map_or(node, |w| w.time_range_node_id as u64),
            };
            sds.outputs.insert(node, (key, binding));
            stored_inputs.insert(
                node,
                output
                    .inputs
                    .stored_outputs
                    .iter()
                    .map(|n| *n as u64)
                    .collect(),
            );
        }
        sds.link_stored_inputs(&stored_inputs)?;
        sds.ingestion_build_order()?;
        Ok(sds)
    }

    /// Records, for each definition, the definitions of the stored outputs
    /// its builder reads, as the plan lists them (`StoredOutputInputs`).
    fn link_stored_inputs(&mut self, inputs: &HashMap<u64, Vec<u64>>) -> Result<(), SdsError> {
        let stored: HashMap<u64, DefinitionKey> = self
            .outputs
            .iter()
            .map(|(node, (key, _))| (*node, key.clone()))
            .collect();
        for definition in self.definitions.values_mut() {
            let mut found = Vec::new();
            for input in inputs.get(&definition.builder).into_iter().flatten() {
                let key = stored.get(input).ok_or(SdsError::UnknownNode(*input))?;
                if !found.contains(key) {
                    found.push(key.clone());
                }
            }
            found.sort();
            definition.stored_inputs = found;
        }
        Ok(())
    }

    /// The definitions built in ingestion runs, each after every definition
    /// it reads (`stored_inputs`). Fails on a cycle, and when an
    /// `IngestionRun` definition reads a `QueryRun` one.
    pub fn ingestion_build_order(&self) -> Result<Vec<DefinitionKey>, SdsError> {
        let mut order = Vec::new();
        let mut state: HashMap<&DefinitionKey, bool> = HashMap::new(); // false: visiting
        fn visit<'a>(
            sds: &'a Sds,
            id: &'a DefinitionKey,
            state: &mut HashMap<&'a DefinitionKey, bool>,
            order: &mut Vec<DefinitionKey>,
        ) -> Result<(), SdsError> {
            match state.get(id) {
                Some(true) => return Ok(()),
                Some(false) => return Err(SdsError::Cycle(id.clone())),
                None => {}
            }
            state.insert(id, false);
            let definition = &sds.definitions[id];
            for input in &definition.stored_inputs {
                if definition.built_at == StoredOutputBuild::IngestionRun
                    && sds.definitions[input].built_at == StoredOutputBuild::QueryRun
                {
                    return Err(SdsError::IngestionReadsQueryRun {
                        definition: id.clone(),
                        input: input.clone(),
                    });
                }
                visit(sds, input, state, order)?;
            }
            state.insert(id, true);
            order.push(id.clone());
            Ok(())
        }
        for id in self.definitions.keys() {
            visit(self, id, &mut state, &mut order)?;
        }
        order.retain(|id| self.definitions[id].built_at == StoredOutputBuild::IngestionRun);
        Ok(order)
    }
}

/// `(start, end]` of `selection`'s relative time, the offsets from the
/// evaluation time a pane covers, when it is a left-open interval.
fn relative_time(selection: &SelectionBox) -> Option<(i64, i64)> {
    match selection.relative_time? {
        (Bound::Excluded(start), Bound::Included(end)) if start < end => Some((start, end)),
        _ => None,
    }
}

/// `selection`'s label constraints in the plan's JSON form; empty when it has
/// none.
fn label_constraints(selection: &SelectionBox) -> LabelConstraints {
    if selection.columns.is_empty() {
        return Arc::from("");
    }
    let columns = SelectionBox {
        columns: selection.columns.clone(),
        relative_time: None,
    };
    Arc::from(serde_json::to_string(&columns).expect("selections serialize"))
}

/// The concrete selection of one summary instance: its definition's label
/// constraints and the time interval `(end_ms - pane_ms, end_ms]` the
/// plan's relative time binds to at evaluation time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Selection {
    pub labels: LabelConstraints,
    pub pane_ms: i64,
    pub end_ms: i64,
}

impl Selection {
    /// The pane `(end_ms - pane_ms, end_ms]`, with no label constraints.
    pub fn ending_at(end_ms: i64, pane_ms: i64) -> Self {
        Self {
            labels: Arc::from(""),
            pane_ms,
            end_ms,
        }
    }
}

impl DefinitionKey {
    /// The concrete selection of this definition's pane ending at `end_ms`.
    pub fn selection_ending_at(&self, end_ms: i64) -> Selection {
        Selection {
            labels: self.labels.clone(),
            pane_ms: self.pane_ms,
            end_ms,
        }
    }
}

/// One stored summary: what it computes and which data it covers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SummaryInstanceKey {
    pub definition: DefinitionId,
    pub selection: Selection,
}

impl SummaryInstanceKey {
    pub fn new(definition: DefinitionId, selection: Selection) -> Self {
        Self {
            definition,
            selection,
        }
    }
}

/// Stored summary instances, by definition and selection. An instance is
/// stored once, when its coverage is complete, and never changes.
#[derive(Debug)]
pub struct SummaryStore<S> {
    instances: BTreeMap<DefinitionId, BTreeMap<Selection, S>>,
}

impl<S> Default for SummaryStore<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> SummaryStore<S> {
    pub fn new() -> Self {
        Self {
            instances: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: SummaryInstanceKey, state: S) {
        self.instances
            .entry(key.definition)
            .or_default()
            .insert(key.selection, state);
    }

    /// The instance of `definition` covering `selection`, if stored. Borrows
    /// both, so a lookup allocates nothing.
    pub fn get(&self, definition: &DefinitionId, selection: &Selection) -> Option<&S> {
        self.instances.get(definition)?.get(selection)
    }

    /// The instance of `definition`'s pane ending at `end_ms`, if stored.
    pub fn get_pane(&self, definition: &DefinitionKey, end_ms: i64) -> Option<&S> {
        self.get(&definition.id, &definition.selection_ending_at(end_ms))
    }

    /// Drops the instances of `definition` with `oldest`'s label constraints
    /// and pane width whose pane ends before `oldest`'s.
    pub fn evict_before(&mut self, definition: &DefinitionId, oldest: &Selection) {
        let Some(instances) = self.instances.get_mut(definition) else {
            return;
        };
        let from = Selection {
            end_ms: i64::MIN,
            ..oldest.clone()
        };
        let expired: Vec<Selection> = instances
            .range(from..oldest.clone())
            .map(|(selection, _)| selection.clone())
            .collect();
        for selection in expired {
            instances.remove(&selection);
        }
    }

    pub fn len(&self) -> usize {
        self.instances.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.instances.values().all(BTreeMap::is_empty)
    }
}
