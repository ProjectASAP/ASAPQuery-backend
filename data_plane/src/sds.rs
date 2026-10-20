//! Stored summaries (SDS): the immutable summaries the data plane stores
//! for a plan, and how a query finds them again.
//!
//! The installed plan lists every output it stores across evaluations
//! (`InstallablePlan::stored_outputs`): which run builds it (an ingestion
//! run, or the query run of the first evaluation that reads it), what it
//! covers, and how long it must stay available. Today the Planner's contract
//! states each stored output's coverage as one pane of raw samples: at
//! evaluation time `T` it covers `(T - end_offset - width, T - end_offset]`,
//! the PromQL range convention.
//!
//! Outputs that differ only in which coverage they read compute the same
//! thing, so they share one [`SummaryDefinition`]: the data plane builds each
//! summary of a definition once, keyed by its coverage end, and every output
//! reads the one it needs. Identity is the SDS's own: the Planner states what
//! each node computes, not which nodes are equal.
//!
//! A stored output may in future be built from other stored outputs
//! (rollups over time or across series). [`Sds::ingestion_build_order`]
//! orders definitions so that each one's stored inputs come first; the
//! precompute engine refuses such a definition until it can build it once
//! all its stored inputs are sealed.

use std::collections::{BTreeMap, HashMap};

use asap_executor::planner::ir::physical_export::{
    PhysicalASAPDAG, PhysicalASAPDAGNode, RawSampleWindow,
};
use asap_executor::planner::ir::schema::Schema;
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::{InstallablePlan, StoredOutputBuild};
use sha2::{Digest, Sha256};

/// Identifies what a stored summary computes, independent of node ids and of
/// which data an output reads: the hash of its coverage definition. Equal for
/// equal computations across plans. When the Planner states each stored
/// output's coverage (ASAPPlanner #781), this becomes the plan's definition
/// id instead of the SDS's own hash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionId(pub String);

/// One computation the data plane stores summary by summary.
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryDefinition {
    pub id: DefinitionId,
    /// Which run builds its summaries: an ingestion run, or the query run of
    /// the first evaluation that needs one (kept once its coverage is
    /// closed). Ingestion when any output of the definition says so.
    pub built_at: StoredOutputBuild,
    /// Coverage width: each stored summary covers `(end - pane_ms, end]`.
    pub pane_ms: i64,
    /// The output that builds a pane; any output of the definition computes
    /// the same, and this is the first in node order.
    pub builder: u64,
    /// The builder's pane: its `TimeRange`, fed one pane's samples, and the
    /// `Scan` that selects them.
    pub window: RawSampleWindow,
    pub output_schema: Schema,
    /// The definitions of the stored outputs the builder reads, which must
    /// be built first. Empty while every stored output reads raw samples.
    pub stored_inputs: Vec<DefinitionId>,
    /// How far back from the newest evaluation time a summary stays available:
    /// the largest `retention_ms` the plan states for the definition's
    /// outputs. A pane that starts earlier is no longer read.
    pub retention_ms: i64,
}

impl SummaryDefinition {
    /// The builder's `TimeRange`, which one pane's samples feed.
    pub fn raw_input(&self) -> u64 {
        self.window.time_range_node_id as u64
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
    /// The output's own `TimeRange` node: where that pane's raw samples
    /// feed when the output is computed at query time.
    pub raw_input: u64,
}

/// The stored summaries of one plan.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Sds {
    /// Every definition, built at ingestion or at query time, by identity.
    pub definitions: BTreeMap<DefinitionId, SummaryDefinition>,
    /// Every stored output, with its definition and coverage.
    pub outputs: BTreeMap<u64, (DefinitionId, PaneBinding)>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SdsError {
    #[error("node {0} is not in the DAG")]
    UnknownNode(u64),
    #[error("stored output {0}'s retention does not fit in i64 milliseconds")]
    RetentionOverflow(u64),
    /// Stored outputs read each other in a cycle.
    #[error("stored outputs read each other in a cycle through {0:?}")]
    Cycle(DefinitionId),
}

impl Sds {
    /// The definitions behind `plan`'s stored outputs, built in an ingestion
    /// run or in a query run and kept.
    pub fn from_plan(plan: &InstallablePlan) -> Result<Self, SdsError> {
        let dag = &plan.dag.dag;
        let nodes: HashMap<u64, &PhysicalASAPDAGNode> =
            dag.nodes.iter().map(|n| (n.id as u64, n)).collect();
        let inputs = inputs_of(dag);
        let mut sds = Sds::default();
        let mut hashes = HashMap::new();
        for output in &plan.stored_outputs {
            let node = output.node_id as u64;
            let (definitions, outputs) = (&mut sds.definitions, &mut sds.outputs);
            let id = DefinitionId(hex(&identity(node, &nodes, &inputs, &mut hashes)?));
            let retention_ms = i64::try_from(output.retention_ms)
                .map_err(|_| SdsError::RetentionOverflow(node))?;
            let output_schema = nodes[&node].output_schema.clone();
            let definition = definitions
                .entry(id.clone())
                .or_insert_with(|| SummaryDefinition {
                    id: id.clone(),
                    built_at: output.built_at,
                    pane_ms: output.pane.width_ms,
                    builder: node,
                    window: output.pane.clone(),
                    output_schema,
                    stored_inputs: Vec::new(),
                    retention_ms,
                });
            if output.built_at == StoredOutputBuild::IngestionRun {
                definition.built_at = StoredOutputBuild::IngestionRun;
            }
            if node < definition.builder {
                definition.builder = node;
                definition.window = output.pane.clone();
            }
            definition.retention_ms = definition.retention_ms.max(retention_ms);
            let binding = PaneBinding {
                offset_ms: output.pane.end_offset_ms,
                raw_input: output.pane.time_range_node_id as u64,
            };
            outputs.insert(node, (id, binding));
        }
        sds.link_stored_inputs(&inputs);
        sds.ingestion_build_order()?;
        Ok(sds)
    }

    /// Records, for each definition, the definitions of the stored outputs
    /// its builder reads: those found below the builder before any raw
    /// input.
    fn link_stored_inputs(&mut self, inputs: &HashMap<u64, Vec<u64>>) {
        let stored: HashMap<u64, DefinitionId> = self
            .outputs
            .iter()
            .map(|(node, (id, _))| (*node, id.clone()))
            .collect();
        for definition in self.definitions.values_mut() {
            let mut found = Vec::new();
            let mut pending: Vec<u64> =
                inputs.get(&definition.builder).cloned().unwrap_or_default();
            let mut seen = std::collections::HashSet::new();
            while let Some(node) = pending.pop() {
                if !seen.insert(node) {
                    continue;
                }
                if let Some(id) = stored.get(&node) {
                    if !found.contains(id) {
                        found.push(id.clone());
                    }
                    continue;
                }
                pending.extend(inputs.get(&node).into_iter().flatten().copied());
            }
            found.sort();
            definition.stored_inputs = found;
        }
    }

    /// The definitions built in ingestion runs, each after every definition
    /// it reads (`stored_inputs`). Fails on a cycle.
    pub fn ingestion_build_order(&self) -> Result<Vec<DefinitionId>, SdsError> {
        let mut order = Vec::new();
        let mut state: HashMap<&DefinitionId, bool> = HashMap::new(); // false: visiting
        fn visit<'a>(
            sds: &'a Sds,
            id: &'a DefinitionId,
            state: &mut HashMap<&'a DefinitionId, bool>,
            order: &mut Vec<DefinitionId>,
        ) -> Result<(), SdsError> {
            match state.get(id) {
                Some(true) => return Ok(()),
                Some(false) => return Err(SdsError::Cycle(id.clone())),
                None => {}
            }
            state.insert(id, false);
            for input in &sds.definitions[id].stored_inputs {
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

/// Each node's inputs, in edge order.
fn inputs_of(dag: &PhysicalASAPDAG) -> HashMap<u64, Vec<u64>> {
    let mut inputs: HashMap<u64, Vec<u64>> = HashMap::new();
    for edge in &dag.edges {
        inputs
            .entry(edge.consumer as u64)
            .or_default()
            .push(edge.producer as u64);
    }
    inputs
}

/// SHA-256 over a node's payload and output schema, then its inputs'
/// identities in edge order. The payload's input ids are zeroed, so the
/// identity does not depend on node numbering. An offset `TimeShift` has its
/// input's identity: the offset says which pane an output reads
/// ([`PaneBinding`]), not what it computes, so the panes of one computation
/// share an identity whether or not their range is shifted.
fn identity(
    node: u64,
    nodes: &HashMap<u64, &PhysicalASAPDAGNode>,
    inputs: &HashMap<u64, Vec<u64>>,
    hashes: &mut HashMap<u64, [u8; 32]>,
) -> Result<[u8; 32], SdsError> {
    if let Some(hash) = hashes.get(&node) {
        return Ok(*hash);
    }
    let dag_node = nodes.get(&node).ok_or(SdsError::UnknownNode(node))?;
    if let Operator::NonASAP(NonASAPOp::TimeShift { shift, .. }) = &dag_node.payload {
        if let (None, Some([input])) = (shift.at, inputs.get(&node).map(Vec::as_slice)) {
            let hash = identity(*input, nodes, inputs, hashes)?;
            hashes.insert(node, hash);
            return Ok(hash);
        }
    }
    let payload = dag_node.payload.map_children(|_| 0);
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&(&payload, &dag_node.output_schema)).expect("serializes"));
    for &input in inputs.get(&node).into_iter().flatten() {
        hasher.update(identity(input, nodes, inputs, hashes)?);
    }
    let hash: [u8; 32] = hasher.finalize().into();
    hashes.insert(node, hash);
    Ok(hash)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Which data one summary instance covers. For now only time: the interval
/// `(end_ms - width, end_ms]`, the width being its definition's coverage
/// width. When the Planner states each stored output's coverage (definition
/// and selection, ASAPPlanner #781), this becomes the plan's selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Selection {
    pub end_ms: i64,
}

impl Selection {
    /// The coverage ending at `end_ms`.
    pub fn ending_at(end_ms: i64) -> Self {
        Self { end_ms }
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
    /// the definition id, so a lookup allocates nothing.
    pub fn get(&self, definition: &DefinitionId, selection: Selection) -> Option<&S> {
        self.instances.get(definition)?.get(&selection)
    }

    /// Drops the instances of `definition` whose coverage ends before
    /// `oldest`'s.
    pub fn evict_before(&mut self, definition: &DefinitionId, oldest: Selection) {
        if let Some(instances) = self.instances.get_mut(definition) {
            *instances = instances.split_off(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.instances.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.instances.values().all(BTreeMap::is_empty)
    }
}
