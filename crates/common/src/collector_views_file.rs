//! The collector-views file (docs/design/collector-sketch-ingest.md, §2):
//! the part of the installed plan the collector runs, which the control
//! plane writes and both the collector and the data plane load. Here
//! because the control plane writes it and the data plane reads it.

use std::collections::BTreeMap;

use asap_types::plan::physical_asap_dag::PhysicalASAPDAG;
use serde::{Deserialize, Serialize};

use crate::PhysicalASAPNodeId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorViewsFile {
    /// `INSTALLABLE_PLAN_WIRE_VERSION` of the planner that wrote `dag`.
    pub planner_wire_version: u32,
    /// `PhysicalASAPDAG::plan_id()` of the installed plan.
    pub plan_id: String,
    /// How long after its end a collector view's window is committed.
    pub commit_grace_ms: u64,
    /// Per metric a `Scan` of the plan reads: whether the backend still
    /// reads its raw samples (a `Scan` of it feeds a node off the
    /// collector, or runs at query time).
    pub backend_reads_raw: BTreeMap<String, bool>,
    /// The collector views: the sub-DAG's outputs whose states the backend
    /// ingests.
    pub outputs: Vec<PhysicalASAPNodeId>,
    /// The collector sub-DAG, nodes and edges as in the installed plan;
    /// `roots = outputs`.
    pub dag: PhysicalASAPDAG,
}
