//! Language-independent execution of ASAPPlanner's post-ASAP DAG.
//!
//! The implementation remains in its compatibility location while PromQL
//! callers migrate. These re-exports give SQL and PromQL one execution API
//! without changing the existing PromQL types or behavior.

pub mod executor {
    pub use crate::query_engines::asap_query_engine::summary_exec::{
        execute, ExecError, ExecOutcome, SummaryExecutor,
    };
}

pub mod context {
    pub use crate::query_engines::asap_query_engine::summary_executor::QueryExecutionContext;
}

pub mod result {
    pub use crate::query_engines::asap_query_engine::summary_executor::SummaryValue;
}

pub mod sds_resolver {
    pub use asap_types::sds::{DataDescriptorId, SummaryDescriptorId};
}
