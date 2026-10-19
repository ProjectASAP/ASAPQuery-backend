//! Stored summaries (SDS): what the data plane stores for a plan's stored
//! outputs, and how a query finds them again.
//!
//! The installed plan lists every output it stores across evaluations
//! (`InstallablePlan::stored_outputs`): when it is built (in an ingestion
//! run, or in the query run that first reads it), the pane of raw samples
//! it is built from, and how long it must stay available. At evaluation
//! time `T` an output's pane covers `(T - end_offset - width, T -
//! end_offset]`, the PromQL range convention.
//!
//! Outputs that differ only in their pane's offset compute the same thing
//! over different panes, so they share one [`SummaryDefinition`]: the data
//! plane builds each pane of a definition once, and every output reads the
//! pane it needs. Identity is the SDS's own: the Planner states what each
//! node computes, not which nodes are equal.

use std::collections::{BTreeMap, HashMap};

use asap_executor::planner::ir::physical_export::{
    PhysicalASAPDAG, PhysicalASAPDAGNode, RawSampleWindow,
};
use asap_executor::planner::ir::schema::Schema;
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::{InstallablePlan, StoredOutputBuild};
use sha2::{Digest, Sha256};

/// Identifies what a stored summary computes, independent of node ids and of
/// which pane an output reads. Equal for equal computations across plans.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionId(pub String);

/// One computation the data plane maintains pane by pane.
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryDefinition {
    pub id: DefinitionId,
    /// Pane width: each stored summary covers `(end - pane_ms, end]`.
    pub pane_ms: i64,
    /// The output that builds a pane; any output of the definition computes
    /// the same, and this is the first in node order.
    pub builder: u64,
    /// The builder's pane: its `TimeRange`, fed one pane's samples, and the
    /// `Scan` that selects them.
    pub window: RawSampleWindow,
    pub output_schema: Schema,
    /// How far back from the newest evaluation time a pane stays available:
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
        newest_ms - self.retention_ms + self.pane_ms
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
    /// Definitions built at ingestion time, by identity.
    pub definitions: BTreeMap<DefinitionId, SummaryDefinition>,
    /// Every output built at ingestion time, with its definition and pane.
    pub outputs: BTreeMap<u64, (DefinitionId, PaneBinding)>,
    /// Definitions of the outputs computed at query time and kept across
    /// evaluations, by identity.
    pub kept_definitions: BTreeMap<DefinitionId, SummaryDefinition>,
    /// Every output kept across evaluations, with its definition and pane.
    pub kept_outputs: BTreeMap<u64, (DefinitionId, PaneBinding)>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SdsError {
    #[error("node {0} is not in the DAG")]
    UnknownNode(u64),
    #[error("stored output {0}'s retention does not fit in i64 milliseconds")]
    RetentionOverflow(u64),
}

impl Sds {
    /// The definitions behind `plan`'s stored outputs: those built in an
    /// ingestion run and those built in a query run and kept.
    pub fn from_plan(plan: &InstallablePlan) -> Result<Self, SdsError> {
        let dag = &plan.dag.dag;
        let nodes: HashMap<u64, &PhysicalASAPDAGNode> =
            dag.nodes.iter().map(|n| (n.id as u64, n)).collect();
        let inputs = inputs_of(dag);
        let mut sds = Sds::default();
        let mut hashes = HashMap::new();
        for output in &plan.stored_outputs {
            let node = output.node_id as u64;
            let (definitions, outputs) = match output.built_at {
                StoredOutputBuild::IngestionRun => (&mut sds.definitions, &mut sds.outputs),
                StoredOutputBuild::QueryRun => (&mut sds.kept_definitions, &mut sds.kept_outputs),
            };
            let id = DefinitionId(hex(&identity(node, &nodes, &inputs, &mut hashes)?));
            let retention_ms = i64::try_from(output.retention_ms)
                .map_err(|_| SdsError::RetentionOverflow(node))?;
            let output_schema = nodes[&node].output_schema.clone();
            let definition = definitions
                .entry(id.clone())
                .or_insert_with(|| SummaryDefinition {
                    id: id.clone(),
                    pane_ms: output.pane.width_ms,
                    builder: node,
                    window: output.pane.clone(),
                    output_schema,
                    retention_ms,
                });
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
        Ok(sds)
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

/// Built panes, by definition and pane end. A pane is stored once, when it
/// is complete, and never changes.
#[derive(Debug, Default)]
pub struct SummaryStore<S> {
    panes: BTreeMap<(DefinitionId, i64), S>,
}

impl<S> SummaryStore<S> {
    pub fn new() -> Self {
        Self {
            panes: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, definition: &DefinitionId, end_ms: i64, state: S) {
        self.panes.insert((definition.clone(), end_ms), state);
    }

    pub fn get(&self, definition: &DefinitionId, end_ms: i64) -> Option<&S> {
        self.panes.get(&(definition.clone(), end_ms))
    }

    /// Drops the panes of `definition` that end before `end_ms`.
    pub fn evict_before(&mut self, definition: &DefinitionId, end_ms: i64) {
        self.panes
            .retain(|(id, end), _| id != definition || *end >= end_ms);
    }

    pub fn len(&self) -> usize {
        self.panes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.panes.is_empty()
    }
}
