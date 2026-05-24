//! Thanos query-engine wrapper.
//!
//! This module owns the public archive query engine, [`ThanosQueryEngine`]
//! (Path A2). The archive tier reuses the `StorageBackend::GorillaObjectStore`
//! routing slot, but the answering engine is Thanos; the superseded
//! in-process Gorilla executor (custom GORILLA1 format) has been deleted.

pub mod forward;

pub use forward::{
    engine_from_env as thanos_engine_from_env, ThanosQueryConfig, ThanosQueryEngine,
    ThanosQueryError, ASAP_THANOS_QUERY_URL_ENV, DATA_SOURCE_THANOS_QUERY_ID,
    DATA_SOURCE_THANOS_QUERY_INFO, DEFAULT_THANOS_QUERY_URL, QUIRK_THANOS_UNREACHABLE,
};
