//! Warm-tier sketch query evaluator (Phase 5 follow-up to PR #122).
//!
//! PR #122 wired the ASAP-tier classification hook in
//! [`crate::query_engines::asap_query_engine::engine::ASAPQueryEngine`]'s
//! `QueryEngine::execute` adapter: parse the PromQL, extract
//! `(metric_name, label_keys)`, look up candidate sids via
//! [`crate::storage_engines::sketch_db::index::SketchStore::instances_matching`],
//! and classify each sid. On `Ghost`/`Unknown`, return
//! `EngineError::CapabilityMiss(SketchStore, …)` so the
//! `EngineRouter` fails over to the archive engine.
//!
//! That hook today still falls through to `handle_query` (legacy
//! datafusion path) on the all-`Hit` case. This module replaces
//! that fall-through with **direct sketch evaluation** from
//! [`SketchStore::query_range`]'s output: deserialize each
//! window's sketch state, dispatch on the per-instance
//! [`crate::storage_engines::sketch_db::index::Capability`], and reduce to a
//! per-window scalar via the canonical sketch query (DDSketch /
//! KLL → quantile, HLL → cardinality estimate, CMS / CountSketch
//! → frequency point query, CMS-with-heap → top-k items).
//!
//! The deserialize + query glue mirrors the per-Capability paths
//! already exercised by `precompute_operators::*_accumulator.rs` —
//! same `asap_sketchlib` library calls so behavior matches the
//! precompute (ingest-side) path bit-for-bit.
//!
//! ## Public surface
//!
//! * [`ASAPTierResult`] — per-series timestamped scalar samples
//!   matching the shape of [`crate::query_engines::query_result::QueryResult::Matrix`],
//!   filled in by `SummaryExecutor` (via
//!   `asap_query_engine::live_serve`/`l4_readout`) — the sole
//!   sketch-serving path; the legacy `SketchReducer` this module used
//!   to also hold is retired (see `asap_tier_result.rs`'s doc).
//!
//! Query answering itself now goes entirely through
//! [`control_plane::asap_tier_analysis::analyze_promql_for_asap_tier`]
//! (candidate/capability resolution) and
//! `asap_query_engine::live_serve::try_serve_from_summary_executor`
//! (the actual `SummaryExecutor` dispatch) — nothing in this module
//! parses PromQL or decodes sketch bytes directly anymore.

pub mod asap_tier_result;
pub mod decoders;
pub mod delta_apply;
pub mod timeline;
pub mod timeline_dispatch;
pub mod window_merger;

pub use asap_tier_result::ASAPTierResult;
