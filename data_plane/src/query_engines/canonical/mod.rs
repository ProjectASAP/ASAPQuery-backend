//! Compatibility re-exports for the existing summary executor.
//!
//! Installed SQL serving executes QueryPlan nodes through its typed relational
//! adapter. These aliases remain for callers of the summary execution API.

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

pub mod exact_promql;
