//! The data plane's boundary types and the `FallbackClient` trait, with the
//! names and shapes of ASAPQuery's engine-boundary design
//! (docs/design/repo-layout.md). Adapters and engines meet here; this crate
//! holds no implementation.
//!
//! It names ASAPPlanner's plan types only (`InstallablePlan`, the DAG's node
//! ids, `QueryLanguage`) and no executor item.
//! `asap-planner`, which defines `InstallablePlan`, still brings the
//! executor and the frontends in at compile time.

mod fallback;
mod plan;
mod query_engine;
mod query_request;
mod query_result;

pub use fallback::{FallbackClient, FallbackError, FallbackResponse};
pub use plan::{InstallError, InstalledPlan, PlanId};
pub use query_engine::{AnswerSource, EngineQueryResult, QueryEngineError};
pub use query_request::{QueryEval, QueryRequest};
pub use query_result::{
    InstantVector, InstantVectorElement, QueryResult, RangeVector, RangeVectorElement, Sample,
    Table, TableValue,
};

/// The largest time, in milliseconds either side of the epoch, the data
/// plane takes a sample at or answers a query at; beyond it a sample is
/// dropped and a query refused, as Prometheus refuses a time it cannot
/// represent. Leaves room for any window offset.
pub const MAX_TIME_MS: i64 = i64::MAX / 4;

/// One series' labels, name to value, `__name__` included, as query results
/// carry them.
pub type LabelSet = std::collections::BTreeMap<String, String>;

// The plan types the boundary names, so callers need no direct Planner
// dependency to use it.
pub use asap_planner::{InstallablePlan, PhysicalASAPNodeId};
pub use asap_types::workload::QueryLanguage;
