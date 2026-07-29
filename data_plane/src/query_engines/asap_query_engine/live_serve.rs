//! The `SummaryExecutor` serving path — the sole way `data_plane` answers
//! a query from the sketch tier. `shadow_compare.rs` (diagnostic-only
//! comparison against the legacy reducer, used to validate this path
//! before it went live) and `sketch_reducer.rs` (the legacy reducer
//! itself) are both retired: neither was "ground truth" any more than
//! this path is, and once this path was the default-on live serving
//! path (design-target-architecture.md Part A), keeping a second,
//! independently-planned answering mechanism around only meant two
//! things could silently disagree with each other, not that either was
//! more trustworthy. See
//! `data_plane/docs/l4node-plan-executor-design.md` and the Phase 2
//! plan's "What 'safe to serve' means, precisely" section for the exact
//! gate this applies.
//!
//! `try_serve_from_summary_executor` returning `None` (flag off, a
//! self-excluded shape like `rate()`/`irate()`/`topk(K, sum
//! by(...)(rate(...)))`/keyed-CMS point-estimate, or no matching
//! registered sid) means the query fails over to archive — there is no
//! other sketch-tier path left to try.

use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::l4_readout::execute_l4_readout;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::query::ASAPTierResult;

/// Fallback accuracy target used only when `l4_lowering.rs`'s
/// observed-family lookup finds nothing registered for the query's
/// metric (in which case no family/params choice here can matter — the
/// query can't be served either way). `data_plane` doesn't carry a
/// per-workload `AccuracyTarget` today (see the design doc's "Rollout"
/// section).
const LIVE_ACCURACY: AccuracyTarget = AccuracyTarget::Epsilon(0.01);

/// Whether the serving cutover is enabled for this process. The env var
/// stays as a kill switch (`ASAP_SUMMARY_EXECUTOR_LIVE=0`/`false`/`off`) —
/// with it set, every query fails over to archive rather than being
/// served from the sketch tier at all (there's no legacy reducer left to
/// fall through to).
///
/// Default flipped to **on** (control_plane/docs/design-target-architecture.md
/// §4/Part A): both unit tests and a real HTTP-level e2e test
/// (`live_serve_hll_global_count_merges_across_sids`,
/// `live_serve_actually_answers_ddsketch_quantile`) already prove
/// correctness for the shapes `try_serve_from_summary_executor` covers,
/// and the grouping-ambiguity problem that gated this default off
/// (empty-`by` ambiguity) is resolved via the real `Reduction::{Reduce,
/// PerEntity}` IR signal, not a heuristic.
pub fn summary_executor_live_enabled() -> bool {
    std::env::var("ASAP_SUMMARY_EXECUTOR_LIVE")
        .map(|v| {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
        })
        .unwrap_or(true)
}

/// Try to serve `query` entirely from `SummaryExecutor`. Returns `None`
/// whenever the query can't be served this way (flag off, a
/// self-excluded shape, no matching registered sid, or lowering/execution
/// failed) — the caller fails over to archive. `Some(...)` means this
/// answered the query.
///
/// This used to carry a third fallback reason: a grouping-ambiguity gate
/// that declined any empty-`by` shape producing >1 group
/// (ASAPController#163). That gate is gone — `Reduction`
/// (ASAPController#165) lets `summary_executor.rs` resolve both halves of
/// the ambiguity correctly on its own, so there is no longer a shape to
/// decline. See `L4ReadoutOutcome`'s doc for the full reasoning.
pub fn try_serve_from_summary_executor(
    index: &SketchStore,
    query: &str,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Option<ASAPTierResult> {
    if !summary_executor_live_enabled() {
        return None;
    }

    let outcome = match execute_l4_readout(index, query, t0_ms, t1_ms, is_cumulative, LIVE_ACCURACY)
    {
        Ok(outcome) => outcome,
        Err(skip) => {
            tracing::debug!(
                query,
                ?skip,
                "live: query not servable from SummaryExecutor, falling back"
            );
            return None;
        }
    };

    tracing::debug!(query, "live: served from SummaryExecutor");
    Some(ASAPTierResult {
        series: outcome.series,
        coverage: outcome.coverage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchInstanceMetadata, SketchKindHandle, SketchSampleState,
    };

    /// Mirrors `shadow_compare.rs`'s `ENV_VAR_LOCK`/`ShadowEnvGuard`
    /// pattern exactly, own env var — `std::env::set_var`/`remove_var`
    /// mutate process-global state and `cargo test` runs this module's
    /// tests on multiple threads in the same process.
    static ENV_VAR_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[allow(dead_code)]
    struct LiveEnvGuard(std::sync::MutexGuard<'static, ()>);

    impl Drop for LiveEnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("ASAP_SUMMARY_EXECUTOR_LIVE");
        }
    }

    fn set_live_env(value: &str) -> LiveEnvGuard {
        let guard = ENV_VAR_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::set_var("ASAP_SUMMARY_EXECUTOR_LIVE", value);
        LiveEnvGuard(guard)
    }

    fn clear_live_env() -> LiveEnvGuard {
        let guard = ENV_VAR_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::remove_var("ASAP_SUMMARY_EXECUTOR_LIVE");
        LiveEnvGuard(guard)
    }

    fn ddsketch_fixture() -> SketchStore {
        let idx = SketchStore::new();
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        idx.register(SketchInstanceMetadata {
            sid: 1,
            metric_name: "latency_ms".to_string(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::DDSketch)),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::DDSketch,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        use asap_sketchlib::{DdSketch, MessagePackCodec};
        let mut sk = DdSketch::new(0.01);
        for i in 1..=100 {
            sk.update(i as f64);
        }
        idx.append_sample(
            1,
            BTreeMap::new(),
            (1_000, 2_000),
            SketchSampleState {
                bytes: sk.to_msgpack().expect("encode DDSketch"),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx
    }

    fn register_hll(idx: &SketchStore, sid: u64, service: &str, items: &[&str]) {
        let cfg = SketchConfig::Hll { precision: 14 };
        let mut group_by_keys = std::collections::BTreeSet::new();
        group_by_keys.insert("service".to_string());
        idx.register(SketchInstanceMetadata {
            sid,
            metric_name: "unique_users".to_string(),
            group_by_keys,
            capability: Some(Capability::CardinalityApprox),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::Hll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        use asap_sketchlib::{HllSketch, HllVariant, MessagePackCodec};
        let mut sk = HllSketch::new(HllVariant::Regular, 14);
        for item in items {
            sk.update(item.as_bytes());
        }
        let mut labels = BTreeMap::new();
        labels.insert("service".to_string(), service.to_string());
        idx.append_sample(
            sid,
            labels,
            (1_000, 2_000),
            SketchSampleState {
                bytes: sk.to_msgpack().expect("encode HLL"),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
    }

    #[test]
    fn flag_explicitly_off_never_serves() {
        // Kill switch: an explicit off-spelling still disables live-serve
        // even though the default (unset) is now on.
        let _guard = set_live_env("0");
        let idx = ddsketch_fixture();
        let result = try_serve_from_summary_executor(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
        );
        assert!(result.is_none(), "flag explicitly off must never serve");
    }

    #[test]
    fn unset_flag_defaults_to_serving() {
        // Default flipped to on (design-target-architecture.md §4/Part A)
        // -- an unset env var must serve, not fall back to the legacy
        // path, for a shape this executor already proves safe.
        let _guard = clear_live_env();
        let idx = ddsketch_fixture();
        let result = try_serve_from_summary_executor(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
        );
        assert!(result.is_some(), "unset flag must default to serving");
    }

    #[test]
    fn flag_on_safe_shape_serves() {
        let _guard = set_live_env("1");
        let idx = ddsketch_fixture();
        let result = try_serve_from_summary_executor(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
        );
        let result = result.expect("unambiguous single-series quantile must serve");
        assert_eq!(result.series.len(), 1);
        assert!(!result.is_empty());
    }

    #[test]
    fn flag_on_global_merge_shape_is_served_merged_not_declined() {
        // Previously `flag_on_ambiguous_shape_falls_back`, asserting
        // `result.is_none()`: the grouping-ambiguity gate declined this
        // shape because an empty `by` couldn't be told apart from "reduce
        // everything" (ASAPController#163). With `Reduction` (#165) the
        // executor resolves it -- `count(...)` lowers to `Reduce([])`, both
        // sids share one group key, and the new path serves the correctly
        // merged answer instead of falling back.
        let _guard = set_live_env("1");
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let result =
            try_serve_from_summary_executor(&idx, "count(unique_users)", 1_000, 2_000, true);
        let result = result.expect(
            "global-merge shape is no longer ambiguous -- it must be served, not declined",
        );
        assert_eq!(
            result.series.len(),
            1,
            "a by-less count() must merge both sids into ONE series, got {:?}",
            result.series
        );
        // Disjoint item sets {a,b,c} + {d,e,f} -> merged cardinality ~6.
        let card = result.series[0].1[0].1;
        assert!(
            (4.0..=8.0).contains(&card),
            "merged cardinality {card} should be ~6 (both sids), not ~3 (one sid)"
        );
    }

    #[test]
    fn flag_on_unservable_query_falls_back() {
        let _guard = set_live_env("1");
        let idx = SketchStore::new();
        let result =
            try_serve_from_summary_executor(&idx, "rate(http_requests_total[5m])", 0, 1000, true);
        assert!(result.is_none());
    }
}
