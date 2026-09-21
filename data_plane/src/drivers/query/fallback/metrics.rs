//! Query-engine hot/cold telemetry counters.
//!
//! The paper's "sketches for hot, exact for cold" story needs a
//! hot-vs-cold breakdown at query time. We mirror the PR #51
//! `queryengine_ingest_samples_blocked_by_schema_barrier_total`
//! pattern: `lazy_static!` registration, one counter per signal,
//! keyed by query shape so the dashboard can split by metric.
//!
//! * **Hot** = the `ASAPQueryEngine` handled the query from live
//!   sketch-backed state.
//! * **Cold** = the query was answered outside the ASAP tier.
//!
//! The "shape" label is the parsed query's root op (`sum`,
//! `count`, `avg`, `selector`, ...) — low-cardinality by design,
//! so the `CounterVec` doesn't explode on the backend. A
//! companion `metric` label carries the first selector's
//! `__name__` so dashboards can slice by metric without the label
//! explosion that a raw-query label would cause.

use lazy_static::lazy_static;
use prometheus::{register_counter_vec, CounterVec};

lazy_static! {
    /// Queries served by the hot (sketch) path, keyed by
    /// `(metric, shape)`. Incremented once per successful
    /// `ASAPQueryEngine::handle_query` that returned `Some(_)`.
    pub static ref QUERIES_HOT_TOTAL: CounterVec = register_counter_vec!(
        "queryengine_hot_queries_total",
        "Queries answered from the sketch (hot) path, keyed by metric + query shape",
        &["metric", "shape"]
    )
    .unwrap();

    /// Queries served by the cold (Gorilla archive) path, keyed
    /// by `(metric, shape)`. Incremented once per successful
    /// archive answer.
    pub static ref QUERIES_COLD_TOTAL: CounterVec = register_counter_vec!(
        "queryengine_cold_queries_total",
        "Queries answered from the cold raw-sample path, keyed by metric + query shape",
        &["metric", "shape"]
    )
    .unwrap();

    /// Raw sample bytes served out of the cold store in response
    /// to queries. Bytes here = the JSON payload of the raw
    /// records scanned (pre-filter), which is the cheapest stable
    /// proxy for "how much cold data the query had to materialise".
    /// Keyed by `(metric, shape)` to match the counter above.
    pub static ref BYTES_SERVED_COLD_TOTAL: CounterVec = register_counter_vec!(
        "queryengine_cold_bytes_served_total",
        "Raw sample bytes scanned from the cold tier in answering queries",
        &["metric", "shape"]
    )
    .unwrap();
}
