//! Warm-tier sketch query evaluator (Phase 5 follow-up to PR #122).
//!
//! PR #122 wired the warm-tier classification hook in
//! [`crate::query_engines::asap_query_engine::engine::ASAPQueryEngine`]'s
//! `QueryEngine::execute` adapter: parse the PromQL, extract
//! `(metric_name, label_keys)`, look up candidate sids via
//! [`crate::stores::sketch_db::store::SketchStore::instances_matching`],
//! and classify each sid. On `Ghost`/`Unknown`, return
//! `EngineError::CapabilityMiss(SketchStore, …)` so the
//! `EngineRouter` fails over to the archive engine.
//!
//! That hook today still falls through to `handle_query` (legacy
//! datafusion path) on the all-`Hit` case. This module replaces
//! that fall-through with **direct sketch evaluation** from
//! [`SketchStore::query_range`]'s output: deserialize each
//! window's sketch state, dispatch on the per-instance
//! [`crate::stores::sketch_db::store::Capability`], and reduce to a
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
//! * [`SketchReducer`] — wraps a `&SketchStore`, takes a
//!   pre-classified slice of all-`Hit` sids + a function name +
//!   args + time bounds, returns a [`WarmTierResult`].
//! * [`WarmTierError`] — distinguishes "warm-tier doesn't support
//!   this function/capability" (router falls over to archive)
//!   from "decode failure" (defensive — also fall over) and
//!   "no data in window" (router falls over).
//! * [`WarmTierResult`] — per-series timestamped scalar samples
//!   matching the shape of [`crate::query_engines::query_result::QueryResult::Matrix`].
//!
//! ## Controller unification (PromQL-shape recognition)
//!
//! The PromQL → `(function_name, args)` AST walker that used to live
//! here in `promql_extract.rs` has been folded into
//! [`controller::warm_tier_analysis::analyze_promql_for_warm_tier`].
//! That function is the single owner of "is this PromQL
//! warm-tier-answerable" knowledge — it returns a
//! [`controller::warm_tier_analysis::WarmTierAnalysis`] enumerating
//! the warm-tier-servable sub-expressions and the explicit
//! [`controller::warm_tier_analysis::UnsupportedReason`] for the rest.
//! The reducer keys off the analyzer's `required_capability` rather
//! than re-string-matching the PromQL function name.
//!
//! Phase-5 hybrid stitching (warm `[t0..t1']` + archive
//! `[t1'..t1]`) and per-window iteration (rather than today's
//! per-sample evaluate-then-merge) remain follow-ups.
//!
//! 2026-05 follow-ups landed here:
//! * **TODO 1**: CMS-with-heap top-k. `Capability::FrequencyTopk(CmsWithHeap)`
//!   reads the embedded heap directly; CMS / CountSketch without a heap
//!   surface as `WarmTierError::MissingHeap` and fail over to archive.
//! * **TODO 2**: Delta encoding stitching. `ProtoDelta` / `MsgpackDelta`
//!   are now applied via [`delta_apply`] — see that module's docs for the
//!   per-window vs cumulative modes (selected by function name).
//! * **TODO 3**: Hybrid warm+archive stitch. [`WarmTierResult::coverage`]
//!   reports the actual `(min_window_start_ms, max_window_end_ms)` the
//!   reducer covered so `ASAPQueryEngine` can stitch the missing prefix /
//!   suffix from the archive engine.

pub mod decoders;
pub mod delta_apply;
pub mod sketch_reducer;

#[cfg(test)]
pub mod tests;

pub use sketch_reducer::{SketchReducer, WarmTierError, WarmTierResult};
