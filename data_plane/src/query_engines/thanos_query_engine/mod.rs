//! Thanos query-engine wrapper.
//!
//! This module owns the public archive query engine, [`ThanosQueryEngine`].
//! Gorilla object storage and the legacy in-process Gorilla executor live
//! under [`crate::storage_engines::gorilla_object_store`].

pub mod forward;

pub use forward::{
    engine_from_env as thanos_engine_from_env, ThanosQueryConfig, ThanosQueryEngine,
    ThanosQueryError, ASAP_THANOS_QUERY_URL_ENV, DATA_SOURCE_THANOS_QUERY_ID,
    DATA_SOURCE_THANOS_QUERY_INFO, DEFAULT_THANOS_QUERY_URL, QUIRK_THANOS_UNREACHABLE,
};
