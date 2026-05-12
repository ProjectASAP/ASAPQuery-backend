//! Placeholder for simple-engine tests.
//!
//! Step-1 of the JSONL deprecation refactor moved
//! `query-engines/simple_engine.rs` to `query-engines/asap_query/engine.rs`. The
//! engine's tests live inline in [`super::engine`] (~6 distinct
//! `#[cfg(test)] mod tests { ... }` blocks, each pinning a
//! specific dispatch axis). They are exercised under
//! `crate::query_engines::asap_query_engine::engine::tests` rather than this file
//! to preserve `git blame` continuity across the move.
//!
//! Step-2 (Prometheus-block format + Thanos store-gateway) can
//! pull the inline test blocks out into this file once the
//! engine's data model stabilises.
