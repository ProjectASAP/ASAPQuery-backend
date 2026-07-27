//! The actual `SummaryExecutor` serving cutover — unlike
//! `shadow_compare.rs` (diagnostic only, never affects what's served),
//! `try_serve_from_summary_executor` returning `Some(...)` means the
//! caller uses THIS answer instead of calling the legacy
//! `SketchReducer` path. See
//! `data_plane/docs/l4node-plan-executor-design.md` and the Phase 2
//! plan's "What 'safe to serve' means, precisely" section for the exact
//! gate this applies.

use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::l4_readout::execute_l4_readout;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::query::ASAPTierResult;

/// Fixed accuracy target for this phase — mirrors
/// `shadow_compare::SHADOW_ACCURACY`; `data_plane` doesn't carry a
/// per-workload `AccuracyTarget` today (see the design doc's "Rollout"
/// section).
const LIVE_ACCURACY: AccuracyTarget = AccuracyTarget::Epsilon(0.01);

/// Whether the actual serving cutover is enabled for this process.
/// Mirrors `shadow_compare::shadow_summary_executor_enabled`'s exact
/// mechanics, own flag, own default (off) — this is a materially
/// riskier switch than shadow mode (it changes what's served, not just
/// what's logged), so it must never be implied by the shadow flag.
pub fn summary_executor_live_enabled() -> bool {
    std::env::var("ASAP_SUMMARY_EXECUTOR_LIVE")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

/// Try to serve `query` entirely from `SummaryExecutor`. Returns `None`
/// whenever the caller should fall back to the legacy path exactly as
/// it does today (flag off, lowering/execution failed, or the
/// grouping-ambiguity gate tripped — see `L4ReadoutOutcome::ambiguous_merge_risk`'s
/// doc) — `None` here is indistinguishable from Phase 1's shadow-only
/// behavior. `Some(...)` means the new path answered and the caller
/// must NOT also call the legacy reducer for this candidate.
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

    if outcome.ambiguous_merge_risk {
        tracing::debug!(
            query,
            "live: ambiguous global-merge shape (ASAPController#163), falling back to legacy path"
        );
        return None;
    }

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
    fn flag_off_never_serves() {
        let _guard = clear_live_env();
        let idx = ddsketch_fixture();
        let result = try_serve_from_summary_executor(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
        );
        assert!(result.is_none(), "flag off must never serve");
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
    fn flag_on_ambiguous_shape_falls_back() {
        let _guard = set_live_env("1");
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let result =
            try_serve_from_summary_executor(&idx, "count(unique_users)", 1_000, 2_000, true);
        assert!(
            result.is_none(),
            "ambiguous global-merge shape must fall back to legacy, not serve a possibly-wrong answer"
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
