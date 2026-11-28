//! Which part of the installed plan the collector runs
//! (docs/design/collector-sketch-ingest.md, §1 and §2): the collector's
//! declared capabilities ([`CollectorCapabilities`]), the bottom-up rule
//! that assigns plan nodes to it ([`collector_sub_dag`]), and the
//! collector-views file both the collector and the data plane load
//! ([`common::CollectorViewsFile`]).
//!
//! A node is assigned to the collector if and only if (a) its operator and
//! arguments are within the capabilities, (b) every input is assigned (a
//! raw `Scan` is assigned by (a) alone), (c) it runs at ingestion, and (d)
//! it is correct on one collector instance's partition of the series. The
//! assigned nodes whose output leaves the collector (the producers of cut
//! edges) are its outputs: a `Scan` output is the raw path, any other must
//! be a materialized view, or it and every assigned node above it are
//! unassigned, until each output is a `Scan` or a view.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use asap_planner::{
    DeploymentCapabilities, ExecutionTiming, InstallablePlan, InstallablePlanError,
    PhysicalASAPDAG, PhysicalASAPNodeId, SupportedSummary, INSTALLABLE_PLAN_WIRE_VERSION,
};
use asap_types::ir::operator::arguments::{Reduction, Source};
use asap_types::ir::operator::kind::OperatorKind;
use asap_types::ir::scalar::{ColumnRef, CompareOpKind, ScalarValue};
use asap_types::ir::schema::FieldDataType;
use asap_types::ir::{ASAPOp, NonASAPOp, Operator, ScalarExpr};
use asap_types::plan::physical_asap_dag::PhysicalASAPDAGNode;
use asap_types::summary_type::{SketchAlgorithm, SummaryInputExpr, SummaryUpdate};
use common::CollectorViewsFile;
use serde::Deserialize;

/// The `capabilities_version` this control plane reads.
const CAPABILITIES_VERSION: u32 = 1;

/// What `asap_sketches --print-capabilities` prints: what the collector can
/// run, in the planner's vocabulary, each capability stated once.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorCapabilities {
    pub capabilities_version: u32,
    pub executor: CollectorExecutor,
    /// Comparisons of a label column with a `Utf8` literal the collector
    /// evaluates, for every operator that takes a predicate.
    pub predicate_ops: Vec<CompareOpKind>,
    pub operators: Vec<OperatorCapability>,
    pub placement: Placement,
    /// The longest time after a window's end before the collector emits it.
    pub window_close_delay_ms: u64,
}

/// `executor`: the planner's `DeploymentCapabilities` with its serde,
/// except that each `supported_summaries` entry is a [`CollectorSummary`].
#[derive(Debug, Clone, PartialEq)]
pub struct CollectorExecutor {
    pub supported_summaries: Vec<CollectorSummary>,
    /// Every other field, with the planner's serde (unknown fields
    /// refused); its own `supported_summaries` is `None`. The rule reads
    /// none of them yet.
    pub rest: DeploymentCapabilities,
}

/// By hand, not `#[serde(flatten)]`: `DeploymentCapabilities` denies
/// unknown fields, which a flattened struct cannot.
impl<'de> Deserialize<'de> for CollectorExecutor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let mut fields = serde_json::Map::deserialize(deserializer)?;
        let summaries = fields
            .remove("supported_summaries")
            .ok_or_else(|| D::Error::missing_field("supported_summaries"))?;
        Ok(Self {
            supported_summaries: serde_json::from_value(summaries).map_err(D::Error::custom)?,
            rest: serde_json::from_value(serde_json::Value::Object(fields))
                .map_err(D::Error::custom)?,
        })
    }
}

/// An entry of `executor.supported_summaries`: the planner's
/// `SupportedSummary`, unchanged, and what only the collector states.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorSummary {
    pub summary: SupportedSummary,
    pub input: SummaryInputKind,
    /// The ranges of θ's parameters the collector accepts, by name.
    pub parameters: BTreeMap<String, ParameterRange>,
}

/// What a summary is built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryInputKind {
    /// Each sample's value: `{"item": null, "weight": {"Column": "SampleValue"}}`.
    Value,
    /// One label column as the item.
    Label,
}

/// A parameter's accepted range; an absent bound does not bind.
#[derive(Debug, Clone, Copy, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterRange {
    pub gt: Option<f64>,
    pub ge: Option<f64>,
    pub lt: Option<f64>,
    pub le: Option<f64>,
}

impl ParameterRange {
    fn contains(&self, value: f64) -> bool {
        self.gt.is_none_or(|bound| value > bound)
            && self.ge.is_none_or(|bound| value >= bound)
            && self.lt.is_none_or(|bound| value < bound)
            && self.le.is_none_or(|bound| value <= bound)
    }
}

/// One operator the collector runs, with the limits of that operator only.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorCapability {
    pub kind: OperatorKind,
    /// `Scan`: the source variants it reads, e.g. `TimeSeries`.
    #[serde(default)]
    pub sources: Vec<String>,
    /// `SummaryTumblingWindowChain` and `SummaryAgg`: the `Reduction`
    /// variants it builds (`PerEntity`, `Reduce`).
    #[serde(default)]
    pub reductions: Vec<String>,
}

impl OperatorCapability {
    /// `Err` for a limit that does not apply to its kind or names no
    /// variant.
    fn check(&self) -> Result<(), String> {
        let summary_build = matches!(
            self.kind,
            OperatorKind::SummaryTumblingWindowChain | OperatorKind::SummaryAgg
        );
        if !self.sources.is_empty() && self.kind != OperatorKind::Scan {
            return Err(format!("sources on {:?}", self.kind));
        }
        if !self.reductions.is_empty() && !summary_build {
            return Err(format!("reductions on {:?}", self.kind));
        }
        if let Some(unknown) = self
            .sources
            .iter()
            .find(|s| !["TimeSeries", "Table"].contains(&s.as_str()))
        {
            return Err(format!("unknown source {unknown}"));
        }
        if let Some(unknown) = self
            .reductions
            .iter()
            .find(|r| !["PerEntity", "Reduce"].contains(&r.as_str()))
        {
            return Err(format!("unknown reduction {unknown}"));
        }
        Ok(())
    }
}

/// How series are partitioned across collector instances.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub partitioned_by: PartitionedBy,
}

/// `"series"`: all samples of one series reach one instance.
/// `{"labels": [..]}`: all series with equal values of those labels do.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionedBy {
    Series,
    Labels(Vec<String>),
}

/// Why a node is not on the collector, for the command's report.
#[derive(Debug, Clone, PartialEq)]
pub enum NotOnCollector {
    /// Rule (a), with what is outside the capabilities.
    NotInCapabilities(String),
    /// Rule (b).
    InputNotOnCollector,
    /// Rule (c).
    NotAtIngestion,
    /// Rule (d).
    NotLocalToPartition,
    /// v1: an output that is not a materialized view, or a node above one.
    OutputNotAView,
}

/// What the commit grace of a collector view's window adds to the
/// collector's declared close delay (`window_close_delay_ms`): its export
/// interval and a clock skew allowance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitGrace {
    pub export_interval_ms: u64,
    pub clock_skew_allowance_ms: u64,
}

impl Default for CommitGrace {
    /// Today's collector pipelines export each message at once; hosts run
    /// NTP: 2 s of skew allowed.
    fn default() -> Self {
        Self {
            export_interval_ms: 0,
            clock_skew_allowance_ms: 2_000,
        }
    }
}

/// Errors reading the capabilities.
#[derive(Debug, thiserror::Error)]
pub enum CapabilitiesError {
    #[error("capabilities_version {0} is not {CAPABILITIES_VERSION}")]
    Version(u32),
    #[error("{0}")]
    Parse(#[from] serde_json::Error),
    #[error("operators: {0}")]
    Operators(String),
}

impl CollectorCapabilities {
    /// The capabilities in `text` (JSON); refuses a version this control
    /// plane does not know.
    pub fn from_json(text: &str) -> Result<Self, CapabilitiesError> {
        let capabilities: Self = serde_json::from_str(text)?;
        if capabilities.capabilities_version != CAPABILITIES_VERSION {
            return Err(CapabilitiesError::Version(
                capabilities.capabilities_version,
            ));
        }
        let mut kinds = BTreeSet::new();
        for operator in &capabilities.operators {
            operator.check().map_err(CapabilitiesError::Operators)?;
            if !kinds.insert(format!("{:?}", operator.kind)) {
                return Err(CapabilitiesError::Operators(format!(
                    "{:?} listed twice",
                    operator.kind
                )));
            }
        }
        Ok(capabilities)
    }

    fn operator(&self, kind: OperatorKind) -> Option<&OperatorCapability> {
        self.operators.iter().find(|op| op.kind == kind)
    }
}

/// The installed plan's collector sub-DAG, as the collector-views file
/// carries it, and why each other node is not on the collector (in node
/// order; the first rule it fails, in the order (a)–(d)). Errs for a plan
/// whose views do not derive, or whose cut does not stand as a DAG of its
/// own.
pub fn collector_sub_dag(
    plan: &InstallablePlan,
    capabilities: &CollectorCapabilities,
    grace: CommitGrace,
) -> Result<
    (
        CollectorViewsFile,
        Vec<(PhysicalASAPNodeId, NotOnCollector)>,
    ),
    InstallablePlanError,
> {
    let dag = &plan.dag;
    let index = Index::new(dag);
    let views: BTreeSet<PhysicalASAPNodeId> = plan
        .materialized_views()?
        .into_iter()
        .map(|view| view.node_id)
        .collect();
    let mut assigned: BTreeSet<PhysicalASAPNodeId> = BTreeSet::new();
    let mut reasons: BTreeMap<PhysicalASAPNodeId, NotOnCollector> = BTreeMap::new();
    for &id in &index.bottom_up {
        let node = index.nodes[&id];
        let reason = within_capabilities(node, &index, capabilities)
            .err()
            .map(NotOnCollector::NotInCapabilities)
            .or_else(|| {
                (!index
                    .inputs(id)
                    .iter()
                    .all(|input| assigned.contains(input)))
                .then_some(NotOnCollector::InputNotOnCollector)
            })
            .or_else(|| {
                (node.output_state.timing != ExecutionTiming::IngestionTime)
                    .then_some(NotOnCollector::NotAtIngestion)
            })
            .or_else(|| {
                (!local_to_partition(node, &index, &capabilities.placement.partitioned_by))
                    .then_some(NotOnCollector::NotLocalToPartition)
            });
        match reason {
            Some(reason) => {
                reasons.insert(id, reason);
            }
            None => {
                assigned.insert(id);
            }
        }
    }
    // v1: every output that is not a `Scan` must be a view; otherwise it
    // and every assigned node above it go back to the backend.
    loop {
        let bad = assigned.iter().copied().find(|&id| {
            !index.is_scan(id) && index.is_output(id, &assigned) && !views.contains(&id)
        });
        let Some(bad) = bad else { break };
        for id in index.above(bad) {
            if assigned.remove(&id) {
                reasons.insert(id, NotOnCollector::OutputNotAView);
            }
        }
    }
    let outputs: Vec<PhysicalASAPNodeId> = assigned
        .iter()
        .copied()
        .filter(|&id| !index.is_scan(id) && index.is_output(id, &assigned))
        .collect();
    // The sub-DAG: the assigned nodes other than `Scan`s, and the `Scan`s
    // that feed one.
    let on_collector: BTreeSet<PhysicalASAPNodeId> = assigned
        .iter()
        .copied()
        .filter(|&id| {
            !index.is_scan(id)
                || index
                    .consumers(id)
                    .iter()
                    .any(|c| assigned.contains(c) && !index.is_scan(*c))
        })
        .collect();
    let sub_dag = PhysicalASAPDAG {
        nodes: dag
            .nodes
            .iter()
            .filter(|node| on_collector.contains(&node.id))
            .cloned()
            .collect(),
        edges: dag
            .edges
            .iter()
            .filter(|edge| {
                on_collector.contains(&edge.producer) && on_collector.contains(&edge.consumer)
            })
            .cloned()
            .collect(),
        roots: outputs.clone(),
        scalar_roots: Vec::new(),
    };
    // A metric is still read raw when one of its `Scan`s feeds a node off
    // the collector (or is a root), or runs at query time.
    let mut backend_reads_raw: BTreeMap<String, bool> = BTreeMap::new();
    for node in &dag.nodes {
        if let Operator::NonASAP(NonASAPOp::Scan {
            source: Source::TimeSeries { metric },
            ..
        }) = &node.payload
        {
            let read_here = node.output_state.timing != ExecutionTiming::IngestionTime
                || !assigned.contains(&node.id)
                || index.roots.contains(&node.id)
                || index
                    .consumers(node.id)
                    .iter()
                    .any(|c| !on_collector.contains(c));
            *backend_reads_raw.entry(metric.clone()).or_default() |= read_here;
        }
    }
    if !sub_dag.roots.is_empty() {
        // The collector loads it into the planner's types (decision 18).
        sub_dag
            .materialized_views()
            .map_err(InstallablePlanError::from)?;
    }
    let file = CollectorViewsFile {
        planner_wire_version: INSTALLABLE_PLAN_WIRE_VERSION,
        plan_id: dag.plan_id(),
        commit_grace_ms: capabilities.window_close_delay_ms
            + grace.export_interval_ms
            + grace.clock_skew_allowance_ms,
        backend_reads_raw,
        outputs,
        dag: sub_dag,
    };
    Ok((file, reasons.into_iter().collect()))
}

/// The DAG's nodes by id, each node's inputs and consumers, and the nodes
/// in bottom-up order (every input before its consumers).
struct Index<'a> {
    nodes: HashMap<PhysicalASAPNodeId, &'a PhysicalASAPDAGNode>,
    inputs: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>>,
    consumers: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>>,
    bottom_up: Vec<PhysicalASAPNodeId>,
    /// The query roots, operator and scalar: their outputs leave the DAG.
    roots: BTreeSet<PhysicalASAPNodeId>,
}

impl<'a> Index<'a> {
    fn new(dag: &'a PhysicalASAPDAG) -> Self {
        let mut inputs: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>> = HashMap::new();
        let mut consumers: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>> = HashMap::new();
        for edge in &dag.edges {
            inputs.entry(edge.consumer).or_default().push(edge.producer);
            consumers
                .entry(edge.producer)
                .or_default()
                .push(edge.consumer);
        }
        // Depth-first postorder over the inputs: a valid DAG has no cycle.
        let mut bottom_up = Vec::with_capacity(dag.nodes.len());
        let mut placed = BTreeSet::new();
        fn visit(
            id: PhysicalASAPNodeId,
            inputs: &HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>>,
            placed: &mut BTreeSet<PhysicalASAPNodeId>,
            order: &mut Vec<PhysicalASAPNodeId>,
        ) {
            if !placed.insert(id) {
                return;
            }
            for &input in inputs.get(&id).into_iter().flatten() {
                visit(input, inputs, placed, order);
            }
            order.push(id);
        }
        for node in &dag.nodes {
            visit(node.id, &inputs, &mut placed, &mut bottom_up);
        }
        let roots = dag
            .roots
            .iter()
            .copied()
            .chain(
                dag.scalar_roots
                    .iter()
                    .flat_map(|(_, expr)| expr.operator_refs())
                    .copied(),
            )
            .collect();
        Self {
            nodes: dag.nodes.iter().map(|node| (node.id, node)).collect(),
            inputs,
            consumers,
            bottom_up,
            roots,
        }
    }

    fn inputs(&self, id: PhysicalASAPNodeId) -> &[PhysicalASAPNodeId] {
        self.inputs.get(&id).map_or(&[], Vec::as_slice)
    }

    fn consumers(&self, id: PhysicalASAPNodeId) -> &[PhysicalASAPNodeId] {
        self.consumers.get(&id).map_or(&[], Vec::as_slice)
    }

    fn is_scan(&self, id: PhysicalASAPNodeId) -> bool {
        matches!(
            self.nodes[&id].payload,
            Operator::NonASAP(NonASAPOp::Scan { .. })
        )
    }

    /// Whether assigned node `id`'s output leaves the collector: a consumer
    /// is not assigned, or it is a query root.
    fn is_output(&self, id: PhysicalASAPNodeId, assigned: &BTreeSet<PhysicalASAPNodeId>) -> bool {
        self.roots.contains(&id) || self.consumers(id).iter().any(|c| !assigned.contains(c))
    }

    /// `id` and every node above it.
    fn above(&self, id: PhysicalASAPNodeId) -> BTreeSet<PhysicalASAPNodeId> {
        let mut seen = BTreeSet::new();
        let mut pending = vec![id];
        while let Some(id) = pending.pop() {
            if seen.insert(id) {
                pending.extend_from_slice(self.consumers(id));
            }
        }
        seen
    }

    /// The output column names of node `id`.
    fn column_names(&self, id: PhysicalASAPNodeId) -> Vec<&str> {
        self.nodes[&id]
            .output_schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect()
    }
}

/// Rule (a): `node`'s operator is listed, with its arguments within that
/// entry's limits and `predicate_ops`, and any summary it builds or reads
/// within `executor.supported_summaries`. `Err` names what is not.
fn within_capabilities(
    node: &PhysicalASAPDAGNode,
    index: &Index,
    capabilities: &CollectorCapabilities,
) -> Result<(), String> {
    let kind = OperatorKind::of(&node.payload);
    let operator = capabilities
        .operator(kind)
        .ok_or_else(|| format!("operator {kind:?}"))?;
    let predicate = |predicate: &ScalarExpr<PhysicalASAPNodeId>| {
        literal_comparisons_only(predicate, &capabilities.predicate_ops)
    };
    let reduction = |reduction: &Reduction| {
        let name = match reduction {
            Reduction::PerEntity => "PerEntity",
            Reduction::Reduce(_) => "Reduce",
        };
        if operator.reductions.iter().any(|listed| listed == name) {
            Ok(())
        } else {
            Err(format!("reduction {name} of {kind:?}"))
        }
    };
    match &node.payload {
        Operator::NonASAP(NonASAPOp::Scan {
            source, predicates, ..
        }) => {
            let name = match source {
                Source::TimeSeries { .. } => "TimeSeries",
                Source::Table { .. } => "Table",
            };
            if !operator.sources.iter().any(|listed| listed == name) {
                return Err(format!("source {name}"));
            }
            predicates.iter().try_for_each(|p| predicate(&p.0))
        }
        Operator::NonASAP(NonASAPOp::Filter { pred, .. }) => predicate(&pred.0),
        Operator::ASAP(
            ASAPOp::SummaryTumblingWindowChain {
                summary_type,
                input,
                reduction: r,
                filter,
                ..
            }
            | ASAPOp::SummaryAgg {
                summary_type,
                input,
                reduction: r,
                filter,
                ..
            },
        ) => {
            reduction(r)?;
            if let Some(filter) = filter {
                predicate(&filter.0)?;
            }
            within_summaries(summary_type, input, node, capabilities)
        }
        Operator::ASAP(ASAPOp::SummaryEstimate {
            summary_input,
            query,
        }) => {
            let algorithm = index.nodes[summary_input]
                .output_schema
                .fields
                .iter()
                .find_map(|field| match field.dtype {
                    FieldDataType::Sketch(algorithm) => Some(algorithm),
                    _ => None,
                })
                .ok_or("an estimate of no sketch")?;
            let entry = summary_entry(algorithm, capabilities)?;
            if entry.summary.readouts.contains(&query.kind()) {
                Ok(())
            } else {
                Err(format!("readout {:?} of {algorithm:?}", query.kind()))
            }
        }
        _ => Ok(()),
    }
}

/// The capabilities' entry of sketch `algorithm`.
fn summary_entry(
    algorithm: SketchAlgorithm,
    capabilities: &CollectorCapabilities,
) -> Result<&CollectorSummary, String> {
    capabilities
        .executor
        .supported_summaries
        .iter()
        .find(|entry| entry.summary.summary_type == algorithm)
        .ok_or_else(|| format!("summary {algorithm:?}"))
}

/// A summary build's type, input and θ (`node.parameters`) within the
/// capabilities' summaries.
fn within_summaries(
    summary_type: &FieldDataType,
    input: &SummaryUpdate,
    node: &PhysicalASAPDAGNode,
    capabilities: &CollectorCapabilities,
) -> Result<(), String> {
    let FieldDataType::Sketch(algorithm) = summary_type else {
        return Err(format!("summary {summary_type:?}"));
    };
    let entry = summary_entry(*algorithm, capabilities)?;
    let input_kind = match (&input.item, &input.weight) {
        (None, SummaryInputExpr::Column(ColumnRef::SampleValue)) => Some(SummaryInputKind::Value),
        (Some(SummaryInputExpr::Column(ColumnRef::Named(_))), _) => Some(SummaryInputKind::Label),
        _ => None,
    };
    if input_kind != Some(entry.input) {
        return Err(format!("input {input:?} of {algorithm:?}"));
    }
    // θ by parameter name through its serde form, `{"<Algorithm>":
    // {"<parameter>": value, ..}}`: the planner's `SketchParams` has no
    // accessor by name.
    let theta = serde_json::to_value(&node.parameters).map_err(|e| e.to_string())?;
    let values = theta
        .as_object()
        .and_then(|by_algorithm| by_algorithm.values().next())
        .and_then(serde_json::Value::as_object);
    for (name, range) in &entry.parameters {
        let value = values
            .and_then(|values| values.get(name))
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| format!("{algorithm:?} without parameter {name}"))?;
        if !range.contains(value) {
            return Err(format!("{algorithm:?} {name} = {value}"));
        }
    }
    Ok(())
}

/// A predicate the collector evaluates: a conjunction of comparisons of a
/// column with a `Utf8` literal (a label matcher), each with an op of
/// `ops`.
fn literal_comparisons_only<C>(
    predicate: &ScalarExpr<C>,
    ops: &[CompareOpKind],
) -> Result<(), String> {
    match predicate {
        ScalarExpr::BoolAnd(all) => all
            .iter()
            .try_for_each(|part| literal_comparisons_only(part, ops)),
        ScalarExpr::Compare {
            left, op, right, ..
        } => match (&**left, &**right) {
            (ScalarExpr::Column(_), ScalarExpr::Literal(ScalarValue::Utf8(_)))
            | (ScalarExpr::Literal(ScalarValue::Utf8(_)), ScalarExpr::Column(_))
                if ops.contains(op) =>
            {
                Ok(())
            }
            _ => Err(format!("predicate op {op:?}")),
        },
        _ => Err("a predicate that is not a conjunction of label comparisons".into()),
    }
}

/// Rule (d): whether `node` is correct on one instance's series, as
/// `partitioned_by` places them. Operators that keep each series' rows to
/// that series are; one grouping series into groups `G` only when each
/// group is on one instance (`{"labels": L}` with `L ⊆ G`); any other
/// operator (joins, binary operators, sorting, limits, populations, ...)
/// is not.
fn local_to_partition(
    node: &PhysicalASAPDAGNode,
    index: &Index,
    partitioned_by: &PartitionedBy,
) -> bool {
    // The input grouped and the group keys' column indexes in it.
    let (child, keys, without): (PhysicalASAPNodeId, Vec<usize>, bool) = match &node.payload {
        Operator::NonASAP(
            NonASAPOp::Scan { .. }
            | NonASAPOp::Filter { .. }
            | NonASAPOp::Project { .. }
            | NonASAPOp::TimeRange { .. }
            | NonASAPOp::TimeShift { .. }
            | NonASAPOp::TimeRangeCover { .. },
        )
        | Operator::ASAP(
            ASAPOp::SummaryEstimate { .. }
            | ASAPOp::FinalizeExactAccumulator { .. }
            | ASAPOp::SummaryMergeWindows { .. }
            | ASAPOp::SummaryMerge { group_by: None, .. },
        ) => return true,
        Operator::NonASAP(NonASAPOp::Aggregate {
            reduction, child, ..
        })
        | Operator::ASAP(
            ASAPOp::SummaryTumblingWindowChain {
                reduction, child, ..
            }
            | ASAPOp::SummaryAgg {
                reduction, child, ..
            },
        ) => match reduction {
            Reduction::PerEntity => return true,
            Reduction::Reduce(keys) => (*child, keys.keys().to_vec(), keys.is_without()),
        },
        Operator::ASAP(ASAPOp::SummaryMerge {
            children,
            group_by: Some(keys),
        }) => match children.as_slice() {
            [child] => (*child, keys.clone(), false),
            _ => return false,
        },
        _ => return false,
    };
    let PartitionedBy::Labels(labels) = partitioned_by else {
        return false;
    };
    let columns = index.column_names(child);
    let named: BTreeSet<&str> = keys
        .iter()
        .filter_map(|&column| columns.get(column).copied())
        .collect();
    labels.iter().all(|label| {
        let in_keys = named.contains(label.as_str());
        if without {
            columns.contains(&label.as_str()) && !in_keys
        } else {
            in_keys
        }
    })
}
