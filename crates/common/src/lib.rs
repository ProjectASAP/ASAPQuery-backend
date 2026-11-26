//! The data plane's boundary types and traits, with the names and shapes of
//! ASAPQuery's engine-boundary design (docs/design/repo-layout.md). Adapters,
//! engines and stores meet here; this crate holds no implementation.
//!
//! It names ASAPPlanner's plan types only (`InstallablePlan`, the DAG's node
//! ids, `RawDataRetention`, `QueryLanguage`, `Source`) and no executor item.
//! `asap-planner`, which defines `InstallablePlan`, still brings the
//! executor and the frontends in at compile time.

mod fallback;
mod ingest;
mod plan;
mod query_engine;
mod query_request;
mod query_result;
mod store;

pub use fallback::{FallbackClient, FallbackError, FallbackResponse};
pub use ingest::{IngestEngine, IngestError, RawDataBatch};
pub use plan::{InstallError, InstalledPlan, PlanId};
pub use query_engine::{AnswerSource, EngineQueryResult, QueryEngine, QueryEngineError};
pub use query_request::{QueryEval, QueryRequest};
pub use query_result::{
    InstantVector, InstantVectorElement, QueryResult, RangeVector, RangeVectorElement, Sample,
    Table, TableValue,
};
pub use store::{
    RawDataStore, ViewId, ViewInstance, ViewInstanceKey, ViewInstanceMetadata, ViewState, ViewStore,
};

/// One series' labels, name to value, `__name__` included: on raw data and
/// on query results.
pub type LabelSet = std::collections::BTreeMap<String, String>;

// The plan types the boundary names, so callers need no direct Planner
// dependency to use it.
pub use asap_planner::{InstallablePlan, PhysicalASAPNodeId, RawDataRetention};
pub use asap_types::ir::operator::arguments::Source;
pub use asap_types::workload::QueryLanguage;
