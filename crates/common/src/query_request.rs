use asap_types::workload::QueryLanguage;

/// One parsed query, independent of the protocol it arrived on.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    pub language: QueryLanguage,
    /// The query text, as the client sent it.
    pub query: String,
    pub eval: QueryEval,
}

/// When to evaluate a query, in unix epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEval {
    Instant {
        time_ms: i64,
    },
    /// At `start_ms`, then every `step_ms` up to `end_ms`.
    Range {
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    },
}
