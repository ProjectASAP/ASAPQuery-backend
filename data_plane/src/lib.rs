pub mod drivers;
pub mod precompute_engine;
pub mod query_engines;
pub mod storage_engines;

#[cfg(test)]
pub mod tests;
pub mod utils;

// Re-export commonly used types to avoid glob import conflicts
pub use storage_engines::types::{
    AccumulatorFactory, AggregateCore, AggregationConfig, KeyByLabelValues, Measurement,
    MergeableAccumulator, MultipleSubpopulationAggregate, MultipleSubpopulationAggregateFactory,
    PrecomputedOutput, SerializableToSink, SingleSubpopulationAggregate,
    SingleSubpopulationAggregateFactory,
};

pub use precompute_engine::operators::{
    IncreaseAccumulator, MinMaxAccumulator, MultipleSumAccumulator, SumAccumulator,
};

pub use storage_engines::StoreResult;

pub use query_engines::{ASAPQueryEngine, InstantVector, QueryResult};

pub use drivers::{HttpServer, HttpServerConfig, OtlpReceiver, OtlpReceiverConfig};

pub use precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
pub use precompute_engine::output_sink::SketchStoreSink;
pub use precompute_engine::PrecomputeEngine;

pub use utils::read_streaming_config;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
