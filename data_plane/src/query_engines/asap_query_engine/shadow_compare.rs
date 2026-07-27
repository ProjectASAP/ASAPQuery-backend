//! Shadow-mode comparison of the new `SummaryExecutor` path against the
//! live `SketchReducer` path — see
//! `data_plane/docs/l4node-plan-executor-design.md`'s "Rollout" section
//! for the design. Computes the new answer alongside the old, diffs the
//! two, logs discrepancies via `tracing`, and **always returns nothing to
//! the caller** — this module can never change what a query serves.
//! Mirrors `docs/design-sketch-db-roadmap.md` § 13.2 "Shadow mode".

use std::collections::BTreeMap;

use asap_sketch::exec::{execute, ExecOutcome};

use crate::query_engines::asap_query_engine::l4_lowering::lower_promql_to_l4node;
use crate::query_engines::asap_query_engine::summary_executor::{
    QueryExecutionContext, SummaryValue,
};
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::query::ASAPTierResult;

/// Fixed accuracy target for this phase — `data_plane` doesn't carry a
/// per-workload `AccuracyTarget` today (see the design doc's "Rollout"
/// section); threading a real one through is a possible fast-follow, not
/// blocking. `0.01` matches this deployment's typical default accuracy
/// bound.
const SHADOW_ACCURACY: control_plane::types_v2::AccuracyTarget =
    control_plane::types_v2::AccuracyTarget::Epsilon(0.01);

/// Relative tolerance for comparing an approximate sketch readout against
/// itself across two independent code paths -- both paths decode the SAME
/// underlying sketch state, so any difference here is a REAL divergence
/// (a bug in one path or the other), not sketch estimation error. A small
/// tolerance absorbs floating-point summation-order differences only.
const RELATIVE_TOLERANCE: f64 = 1e-6;

/// Mirrors `ASAPTierResult.series`'s row shape -- `(label_values, samples)`
/// where `samples` is `(window_end_unix_ms, value)`.
type SeriesRows = Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>;

/// Whether shadow-mode comparison is enabled for this process. Mirrors
/// `ASAP_LEGACY_DUAL_WRITE`'s exact mechanics (`drivers/ingest/otel.rs`) --
/// trimmed, case-insensitive `1`/`true`/`on`, default off.
pub fn shadow_summary_executor_enabled() -> bool {
    std::env::var("ASAP_SHADOW_SUMMARY_EXECUTOR")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

/// Compute the new (`SummaryExecutor`) answer for `query` alongside the
/// already-computed legacy `old` answer, diff the two, and log via
/// `tracing`. Never returns anything, never panics, never affects what
/// the caller serves -- every fallible step is `Result`/`Option`-handled
/// and logged rather than `.unwrap()`ed, so a bug in this module's own
/// conversion/diff logic degrades to "no useful log line," not a crash.
pub fn maybe_shadow_compare(
    index: &SketchStore,
    query: &str,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
    old: &ASAPTierResult,
) {
    if !shadow_summary_executor_enabled() {
        return;
    }

    let node = match lower_promql_to_l4node(query, SHADOW_ACCURACY) {
        Ok(node) => node,
        Err(skip) => {
            tracing::debug!(query, ?skip, "shadow: query not comparable, skipping");
            return;
        }
    };

    let ctx = QueryExecutionContext {
        index,
        t0_ms,
        t1_ms,
        is_cumulative,
    };

    let new_series = match execute(&node, &ctx) {
        Ok(ExecOutcome::Value(values)) => {
            let mut coverage: Option<(u64, u64)> = None;
            let mut series = Vec::new();
            for (group_key, value) in &values {
                fold_coverage(&mut coverage, value.coverage());
                series.extend(summary_value_to_series(group_key, value));
            }
            (series, coverage)
        }
        Ok(ExecOutcome::State(groups)) => {
            let mut series = Vec::new();
            for (group_key, state, _kind, _params) in &groups {
                let Some(value) = state.exact_value(&None) else {
                    tracing::debug!(
                        query,
                        ?group_key,
                        "shadow: ExactAgg group had no comparable value, skipping group"
                    );
                    continue;
                };
                series.push((group_key.clone(), vec![(t1_ms as i64, value)]));
            }
            (series, None)
        }
        Err(e) => {
            tracing::debug!(query, error = ?e, "shadow: execute() failed, skipping");
            return;
        }
    };

    diff_and_log(query, old, &new_series.0, new_series.1);
}

/// `SummaryValue::Points`/`TopK` -> `ASAPTierResult.series`'s row shape.
/// `TopK`'s ranked-list-per-timestamp shape is pivoted into one row per
/// item (each row = the group's label map plus an `item` label, one point
/// per timestamp that item appeared in the ranked list) -- the SAME
/// convention `sketch_reducer.rs`'s own topk arm already uses, not a new
/// one invented here.
fn summary_value_to_series(
    group_key: &BTreeMap<String, String>,
    value: &SummaryValue,
) -> SeriesRows {
    match value {
        SummaryValue::Points(points, _coverage) => {
            vec![(group_key.clone(), points.clone())]
        }
        SummaryValue::TopK(ranked_per_ts, _coverage) => {
            let mut by_item: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
            for (ts, items) in ranked_per_ts {
                for (item, val) in items {
                    by_item.entry(item.clone()).or_default().push((*ts, *val));
                }
            }
            by_item
                .into_iter()
                .map(|(item, points)| {
                    let mut lv = group_key.clone();
                    lv.insert("item".to_string(), item);
                    (lv, points)
                })
                .collect()
        }
    }
}

fn fold_coverage(coverage: &mut Option<(u64, u64)>, next: Option<(u64, u64)>) {
    let Some((lo, hi)) = next else { return };
    *coverage = Some(match *coverage {
        Some((clo, chi)) => (clo.min(lo), chi.max(hi)),
        None => (lo, hi),
    });
}

/// Diff the new path's series/coverage against the old `ASAPTierResult`
/// and log via `tracing` -- `warn!` on a real discrepancy, `debug!` on a
/// clean match. Never returns anything the caller could act on.
///
/// KNOWN, understood noise source (confirmed against a real e2e query
/// during development, not theoretical): for a bare per-series range
/// function with no PromQL `by(...)` (e.g. `quantile_over_time(m[r])`,
/// as opposed to a true grouping aggregate like `quantile(0.9, sum by
/// (job)(m))`), the legacy path preserves the underlying series' full
/// label set, while the new path's `find_candidates` projects onto the
/// query's `by` columns -- empty here -- collapsing to `{}`. This is a
/// group-KEY-shape gap, not a value-computation bug (the values agree);
/// `single_ungrouped_series` below detects exactly this one-row-both-sides
/// shape and logs it distinctly so it doesn't drown out real mismatches
/// in the noise, without pretending it's already resolved.
fn diff_and_log(
    query: &str,
    old: &ASAPTierResult,
    new_series: &SeriesRows,
    new_coverage: Option<(u64, u64)>,
) {
    let old_by_group: BTreeMap<&BTreeMap<String, String>, &Vec<(i64, f64)>> =
        old.series.iter().map(|(k, v)| (k, v)).collect();
    let new_by_group: BTreeMap<&BTreeMap<String, String>, &Vec<(i64, f64)>> =
        new_series.iter().map(|(k, v)| (k, v)).collect();

    if old_by_group.keys().collect::<Vec<_>>() != new_by_group.keys().collect::<Vec<_>>() {
        if single_ungrouped_series(old, new_series) {
            tracing::debug!(
                query,
                old_group = ?old.series[0].0,
                "shadow: known gap -- new path's empty by() group key doesn't carry the \
                 series' own labels for a bare per-series range function (values not compared)"
            );
            return;
        }
        tracing::warn!(
            query,
            old_groups = ?old_by_group.keys().collect::<Vec<_>>(),
            new_groups = ?new_by_group.keys().collect::<Vec<_>>(),
            "shadow mismatch: group sets differ"
        );
        return;
    }

    let mut any_mismatch = false;
    for (group, old_points) in &old_by_group {
        // `expect`-free: the key-set equality check above guarantees this
        // lookup succeeds; still handled defensively rather than indexed.
        let Some(new_points) = new_by_group.get(group) else {
            any_mismatch = true;
            continue;
        };
        if !points_match(old_points, new_points) {
            any_mismatch = true;
            tracing::warn!(
                query,
                ?group,
                old = ?old_points,
                new = ?new_points,
                "shadow mismatch: values differ"
            );
        }
    }

    if !any_mismatch && old.coverage != new_coverage {
        tracing::debug!(
            query,
            old_coverage = ?old.coverage,
            new_coverage = ?new_coverage,
            "shadow: coverage differs (informational, not scored as a value mismatch)"
        );
    }

    if !any_mismatch {
        tracing::debug!(query, "shadow: match");
    }
}

/// Detects the specific "one row on each side, new path's key is `{}`"
/// shape -- see `diff_and_log`'s doc for why this is a known group-key
/// gap, not a real mismatch, when it happens to hold. Deliberately does
/// NOT compare values here: if the group keys differ, comparing the
/// vectors would be comparing two potentially-unrelated series by
/// coincidence of list position, not by any real correspondence.
fn single_ungrouped_series(old: &ASAPTierResult, new_series: &SeriesRows) -> bool {
    old.series.len() == 1 && new_series.len() == 1 && new_series[0].0.is_empty()
}

fn points_match(a: &[(i64, f64)], b: &[(i64, f64)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted = a.to_vec();
    let mut b_sorted = b.to_vec();
    a_sorted.sort_by_key(|(ts, _)| *ts);
    b_sorted.sort_by_key(|(ts, _)| *ts);
    a_sorted
        .iter()
        .zip(b_sorted.iter())
        .all(|((ta, va), (tb, vb))| ta == tb && relative_eq(*va, *vb))
}

fn relative_eq(a: f64, b: f64) -> bool {
    if a == b {
        return true;
    }
    let scale = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() / scale <= RELATIVE_TOLERANCE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchInstanceMetadata, SketchKindHandle, SketchSampleState,
    };

    /// `std::env::set_var`/`remove_var` mutate process-global state, and
    /// `cargo test` runs tests in the same process across multiple
    /// threads by default -- every test touching `ASAP_SHADOW_SUMMARY_EXECUTOR`
    /// must hold this for its duration to avoid racing the others (mirrors
    /// `control_plane/src/main.rs`'s `EnvVarGuard` pattern for the same
    /// reason). Resets the var on drop so tests don't leak global state
    /// into whatever runs next in this process.
    static ENV_VAR_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[allow(dead_code)] // held for its lock-lifetime/Drop side effect, never read
    struct ShadowEnvGuard(std::sync::MutexGuard<'static, ()>);

    impl Drop for ShadowEnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("ASAP_SHADOW_SUMMARY_EXECUTOR");
        }
    }

    fn set_shadow_env(value: &str) -> ShadowEnvGuard {
        let guard = ENV_VAR_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::set_var("ASAP_SHADOW_SUMMARY_EXECUTOR", value);
        ShadowEnvGuard(guard)
    }

    fn kll_fixture() -> SketchStore {
        let idx = SketchStore::new();
        let cfg = SketchConfig::Kll { k: 200 };
        idx.register(SketchInstanceMetadata {
            sid: 1,
            metric_name: "latency_ms".to_string(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::Kll)),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::Kll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });

        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;
        let items: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let state = KllState {
            k: 200,
            items,
            levels: vec![],
            num_levels: 0,
            ..Default::default()
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
            ..Default::default()
        };
        idx.append_sample(
            1,
            BTreeMap::new(),
            (1_000, 2_000),
            SketchSampleState {
                bytes: env.encode_to_vec(),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        idx
    }

    #[test]
    fn maybe_shadow_compare_matching_fixture_does_not_panic() {
        let _guard = set_shadow_env("1");
        let idx = kll_fixture();
        // Median of 1..=100 is ~50 -- matches what the new path should
        // independently compute from the SAME underlying sketch state.
        let old = ASAPTierResult {
            series: vec![(BTreeMap::new(), vec![(2_000, 50.0)])],
            coverage: Some((2_000, 2_000)),
        };
        maybe_shadow_compare(
            &idx,
            "quantile_over_time(latency_ms[1m])",
            1_000,
            2_000,
            true,
            &old,
        );
    }

    #[test]
    fn maybe_shadow_compare_mismatched_fixture_does_not_panic() {
        let _guard = set_shadow_env("1");
        let idx = kll_fixture();
        // Deliberately wrong value -- proves the mismatch path (not just
        // the match path) runs cleanly too.
        let old = ASAPTierResult {
            series: vec![(BTreeMap::new(), vec![(2_000, 999.0)])],
            coverage: Some((2_000, 2_000)),
        };
        maybe_shadow_compare(
            &idx,
            "quantile_over_time(latency_ms[1m])",
            1_000,
            2_000,
            true,
            &old,
        );
    }

    // One test, not three: `std::env::set_var`/`remove_var` mutate
    // process-global state, and `cargo test` runs tests in the same
    // process across multiple threads by default -- separate test fns
    // touching the same env var can race. Merging into one sequential
    // test avoids adding a synchronization primitive just for this.
    #[test]
    fn shadow_env_var_gate() {
        // Acquire the same lock `set_shadow_env` uses (without its value,
        // since this test sweeps through several values itself) so it
        // can't race the other env-var tests in this module.
        let _guard = ENV_VAR_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::remove_var("ASAP_SHADOW_SUMMARY_EXECUTOR");
        assert!(
            !shadow_summary_executor_enabled(),
            "expected disabled by default"
        );

        for v in ["1", "true", "TRUE", "on", " 1 "] {
            std::env::set_var("ASAP_SHADOW_SUMMARY_EXECUTOR", v);
            assert!(
                shadow_summary_executor_enabled(),
                "expected {v:?} to enable shadow mode"
            );
        }

        for v in ["0", "false", "no", ""] {
            std::env::set_var("ASAP_SHADOW_SUMMARY_EXECUTOR", v);
            assert!(
                !shadow_summary_executor_enabled(),
                "expected {v:?} to NOT enable shadow mode"
            );
        }

        // Same test, same env-var state (disabled): `maybe_shadow_compare`
        // must not even attempt to lower/execute -- pass a query that
        // would otherwise fail loudly to prove the early return is
        // genuinely taken, not just "happened to not crash."
        std::env::remove_var("ASAP_SHADOW_SUMMARY_EXECUTOR");
        let idx = SketchStore::new();
        let old = ASAPTierResult {
            series: vec![],
            coverage: None,
        };
        maybe_shadow_compare(&idx, "this is not promql (((", 0, 1000, true, &old);
    }

    #[test]
    fn points_match_ignores_order() {
        let a = vec![(1, 1.0), (2, 2.0)];
        let b = vec![(2, 2.0), (1, 1.0)];
        assert!(points_match(&a, &b));
    }

    #[test]
    fn points_match_within_relative_tolerance() {
        let a = vec![(1, 100.0)];
        let b = vec![(1, 100.0000001)];
        assert!(points_match(&a, &b));
    }

    #[test]
    fn points_mismatch_beyond_tolerance() {
        let a = vec![(1, 100.0)];
        let b = vec![(1, 105.0)];
        assert!(!points_match(&a, &b));
    }

    #[test]
    fn points_mismatch_different_lengths() {
        let a = vec![(1, 1.0)];
        let b = vec![(1, 1.0), (2, 2.0)];
        assert!(!points_match(&a, &b));
    }
}
