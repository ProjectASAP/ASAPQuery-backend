#![allow(
    dead_code,
    unused_imports,
    unused_mut,
    unused_variables,
    clippy::bool_assert_comparison,
    clippy::chunks_exact_to_as_chunks,
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::doc_lazy_continuation,
    clippy::field_reassign_with_default,
    clippy::items_after_test_module,
    clippy::len_without_is_empty,
    clippy::manual_checked_ops,
    clippy::match_result_ok,
    clippy::needless_lifetimes,
    clippy::redundant_field_names,
    clippy::result_large_err,
    clippy::suspicious_open_options,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::useless_conversion,
    clippy::useless_format,
    clippy::while_let_loop
)]

pub mod drivers;
pub mod update_sampling;

#[deprecated(note = "Use update_sampling")]
pub use update_sampling as monitor;
pub mod precompute_engine;
pub mod query_engines;
pub mod storage_engines;

pub mod utils;

// Re-export commonly used types to avoid glob import conflicts
pub use storage_engines::types::{
    AggregateCore, KeyByLabelValues, Measurement, MergeableAccumulator,
    MultipleSubpopulationAggregate, PrecomputeMaterialization, PrecomputedOutput,
    SerializableToSink, SingleSubpopulationAggregate,
};

pub use precompute_engine::operators::{
    IncreaseAccumulator, KeyedSumCountAccumulator, MaxAccumulator, MinAccumulator, SumAccumulator,
};

pub use storage_engines::StoreResult;

pub use query_engines::{ASAPQueryEngine, InstantVector, QueryResult};

pub use drivers::ingest::{
    PrometheusRemoteWriteConfig, PrometheusRemoteWriteReceiver, RemoteWriteStats,
};
pub use drivers::{HttpServer, HttpServerConfig, OtlpReceiver, OtlpReceiverConfig};

pub use precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
pub use precompute_engine::output_sink::SketchStoreSink;
pub use precompute_engine::PrecomputeEngine;

pub use utils::read_streaming_config;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
pub mod tests;

pub mod runtime_config;
