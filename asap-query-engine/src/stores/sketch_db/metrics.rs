//! Prometheus metrics exposed on `/metrics` for the sketch DB.
//!
//! Scope: counters / gauges that turn otherwise-silent ingest or
//! eviction behaviour into graphable signals. Registered via
//! `lazy_static` + `prometheus::register_counter_vec!`, so a single
//! `prometheus::gather()` at the HTTP handler picks them up
//! automatically.

use lazy_static::lazy_static;
use prometheus::{register_counter_vec, CounterVec};

lazy_static! {
    /// §6.3 write-side barrier drops, per `agg_id`. Incremented when
    /// the ingest path matches a sample's metric to an agg config
    /// whose `AggSchema` is Retired or Expired, so the sample is
    /// dropped instead of being routed to a worker. A non-zero rate
    /// with no Retired/Expired schemas in the registry means the
    /// reconcile / hot-reload path is lagging — silent drop becomes
    /// a graphable alert condition.
    pub static ref SAMPLES_BLOCKED_BY_SCHEMA_BARRIER: CounterVec = register_counter_vec!(
        "queryengine_ingest_samples_blocked_by_schema_barrier_total",
        "Ingest samples dropped by the §6.3 write-side schema barrier, keyed by agg_id",
        &["agg_id"]
    )
    .unwrap();
}
