//! Stored summaries (SDS): what the data plane stores for a plan's
//! ingestion-time outputs, and how a query finds them again.
//!
//! The Planner's selected DAG marks each output computed at ingestion time
//! (`MatAtIngestion`) and read at query time: the executor's materialization
//! frontier. The control plane installs the DAG with each `PaneChain`
//! expanded into its panes, so each such output reads one tumbling pane of
//! raw samples:
//!
//! ```text
//! frontier node <- ... <- TimeRange(width) <- TimeShift(offset)* <- Scan
//! ```
//!
//! At evaluation time `T` it covers `(T - offset - width, T - offset]`, the
//! PromQL range convention.
//! Outputs that differ only in `offset` compute the same thing over different
//! panes, so they share one [`SummaryDefinition`]: the data plane builds each
//! pane of a definition once, and every output reads the pane it needs.

use std::collections::{BTreeMap, HashMap};

use planner_types::ir::physical_export::{PhysicalASAPDAG, PhysicalASAPDAGNode};
use planner_types::ir::schema::Schema;
use planner_types::ir::{NonASAPOp, Operator};
use planner_types::physical::Materialization;
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
    /// The frontier output that builds a pane; any output of the definition
    /// computes the same, and this is the first in node order.
    pub builder: u64,
    /// The raw input of the build: the `TimeRange` node fed one pane's samples.
    pub raw_input: u64,
    pub output_schema: Schema,
}

/// How one frontier output reads its definition: at evaluation time `T`, the
/// pane ending at `T - offset_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaneBinding {
    pub offset_ms: i64,
}

/// The stored summaries of one plan.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Sds {
    pub definitions: BTreeMap<DefinitionId, SummaryDefinition>,
    /// Every frontier output, with its definition and pane.
    pub outputs: BTreeMap<u64, (DefinitionId, PaneBinding)>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SdsError {
    #[error("node {0} is not in the DAG")]
    UnknownNode(u64),
    #[error("ingestion-time output {node} does not read one pane of one Scan: {reason}")]
    UnsupportedShape { node: u64, reason: String },
    /// The data plane keeps no query-time result across evaluations; it
    /// declares `query_time_results_retained: false`, so the Planner should
    /// not select such a plan.
    #[error("node {0} is kept across evaluations (MatAtQueryKeptAcrossEvaluations), which the data plane does not support")]
    KeptAcrossEvaluations(u64),
}

impl Sds {
    /// The definitions behind `frontier`, the DAG outputs computed at
    /// ingestion time and read at query time.
    pub fn from_dag(dag: &PhysicalASAPDAG, frontier: &[u64]) -> Result<Self, SdsError> {
        let nodes: HashMap<u64, &PhysicalASAPDAGNode> =
            dag.nodes.iter().map(|n| (n.id as u64, n)).collect();
        let mut inputs: HashMap<u64, Vec<u64>> = HashMap::new();
        for edge in &dag.edges {
            inputs
                .entry(edge.consumer as u64)
                .or_default()
                .push(edge.producer as u64);
        }
        if let Some(node) = dag.nodes.iter().find(|n| {
            matches!(
                n.materialization,
                Materialization::MatAtQueryKeptAcrossEvaluations(_)
            )
        }) {
            return Err(SdsError::KeptAcrossEvaluations(node.id as u64));
        }
        let mut sds = Sds::default();
        let mut hashes = HashMap::new();
        for &node in frontier {
            let (raw_input, pane_ms, offset_ms) = pane_of(node, &nodes, &inputs)?;
            let id = DefinitionId(hex(&identity(node, &nodes, &inputs, &mut hashes)?));
            let output_schema = nodes[&node].output_schema.clone();
            let definition =
                sds.definitions
                    .entry(id.clone())
                    .or_insert_with(|| SummaryDefinition {
                        id: id.clone(),
                        pane_ms,
                        builder: node,
                        raw_input,
                        output_schema,
                    });
            if node < definition.builder {
                definition.builder = node;
                definition.raw_input = raw_input;
            }
            sds.outputs.insert(node, (id, PaneBinding { offset_ms }));
        }
        Ok(sds)
    }
}

/// `(raw input, pane width, offset)` of a frontier output: follow its single
/// inputs down to the `TimeRange`, then sum the `TimeShift`s down to the `Scan`.
fn pane_of(
    node: u64,
    nodes: &HashMap<u64, &PhysicalASAPDAGNode>,
    inputs: &HashMap<u64, Vec<u64>>,
) -> Result<(u64, i64, i64), SdsError> {
    let unsupported = |reason: &str| SdsError::UnsupportedShape {
        node,
        reason: reason.into(),
    };
    let only_input = |id: u64| match inputs.get(&id).map(Vec::as_slice) {
        Some([input]) => Ok(*input),
        _ => Err(unsupported(
            "every operator above the Scan must have one input",
        )),
    };
    let mut current = node;
    let (raw_input, pane_ms) = loop {
        match relational(nodes, current)? {
            Some(NonASAPOp::TimeRange { range, .. }) => {
                break (current, range.as_millis() as i64);
            }
            Some(NonASAPOp::TimeShift { .. }) => {
                return Err(unsupported("a TimeShift above the TimeRange"));
            }
            Some(NonASAPOp::Scan { .. }) => return Err(unsupported("no TimeRange")),
            _ => current = only_input(current)?,
        }
    };
    let mut offset_ms = 0;
    current = only_input(raw_input)?;
    loop {
        match relational(nodes, current)? {
            Some(NonASAPOp::TimeShift { shift, .. }) if shift.at.is_none() => {
                offset_ms += shift.offset_ms;
                current = only_input(current)?;
            }
            Some(NonASAPOp::Scan { .. }) => return Ok((raw_input, pane_ms, offset_ms)),
            _ => {
                return Err(unsupported(
                    "only offset TimeShifts between TimeRange and Scan",
                ))
            }
        }
    }
}

fn relational<'a>(
    nodes: &HashMap<u64, &'a PhysicalASAPDAGNode>,
    id: u64,
) -> Result<Option<&'a NonASAPOp<usize>>, SdsError> {
    match &nodes.get(&id).ok_or(SdsError::UnknownNode(id))?.payload {
        Operator::NonASAP(operator) => Ok(Some(operator)),
        Operator::ASAP(_) => Ok(None),
    }
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

    /// Drops the panes of `definition` that end at or before `end_ms`.
    pub fn evict_through(&mut self, definition: &DefinitionId, end_ms: i64) {
        self.panes
            .retain(|(id, end), _| id != definition || *end > end_ms);
    }

    pub fn len(&self) -> usize {
        self.panes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.panes.is_empty()
    }
}
