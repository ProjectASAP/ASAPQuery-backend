//! Placeholder for simple-engine tests.
//!
//! Step-1 of the JSONL deprecation refactor moved
//! `engines/simple_engine.rs` to `engines/simple/engine.rs`. The
//! engine's tests live inline in [`super::engine`] (~6 distinct
//! `#[cfg(test)] mod tests { ... }` blocks, each pinning a
//! specific dispatch axis). They are exercised under
//! `crate::engines::asap_query::engine::tests` rather than this file
//! to preserve `git blame` continuity across the move.
//!
//! Step-2 (Prometheus-block format + Thanos store-gateway) can
//! pull the inline test blocks out into this file once the
//! engine's data model stabilises.
