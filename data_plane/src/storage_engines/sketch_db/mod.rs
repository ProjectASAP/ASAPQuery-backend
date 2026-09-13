//! Summary storage, persistence, query composition, lifecycle, and backfill.
//!
//! The sid-keyed index holds instance metadata and per-window state. Lifecycle
//! services reconcile configured policies and evict expired instances. Backfill
//! rebuilds windows from source samples through its own worker and registry.

pub mod accuracy;
pub mod backfill;
pub mod data;
pub mod index;
pub mod lifecycle;
pub mod metrics;
pub mod persistence;
pub mod query;
pub mod sds;

pub use accuracy::{AccuracyEnvelope, AccuracyKind, AccuracyProfile, PerSegmentAccuracy};
pub use backfill::{
    build_backfilled_accumulator, clickhouse_reader_factory, default_reader_factory,
    noop_reader_factory, BackfillJob, BackfillRegistry, BackfillService, BackfillServiceConfig,
    BackfillServiceHandle, BackfillSource, BackfillStatus, BackfillWindowProcessor, BackfillWorker,
    BackfillWorkerError, ClickHouseReaderConfig, Coverage, CreateError, LabelFilter,
    MockRawSampleReader, PrometheusReader, RawSample, RawSampleReader, RawSampleReaderError,
    ReaderFactory, WindowProcessor,
};
pub use lifecycle::{
    warn_if_retention_inverted, AggStatus, SchemaEvictionConfig, SchemaEvictionHandle,
    SchemaEvictionService, DEFAULT_RETIREMENT_RETENTION,
};
pub use query::timeline::{TimelineCoverage, TimelineSegment};
pub use sds::{
    DataDescriptor, DataDescriptorId, SdsBinding, SummaryDescriptor, SummaryDescriptorId,
    SummaryDescriptorRegistry, SummaryOperator,
};
