//! Autonomous allocation, slice 3: derive the per-metric **knobs** — sampling
//! probability `p` and the CDM delta-transmission threshold `δ` — from the
//! target accuracy `ε`.
//!
//! Slice 2 ([`crate::query_planning`]) answered *which sketches* a query set
//! needs. This slice answers *how to run them*: it attaches `p` and `δ` to each
//! planned metric, completing `(ε, queries) → {sketches, p, δ}`.
//!
//! Two derivations, with an important asymmetry:
//!   * **`p` is fully autonomous** from `(ε, rate)` via the unified ε-floor law
//!     `p = 1/(1+ε²·rate)` — same law the data plane applies per-edge
//!     ([`epsilon_sample_floor`]). `rate` is the per-window update count.
//!   * **`δ` is only *sized* by ε.** The CDM threshold `τ` (the value of
//!     interest the monitor alerts on) is a *user/query input*, not derivable
//!     from ε. Given `τ` and the site count `k`, the even-split per-site slack
//!     is `δ = ε·τ / k`. Metrics with no monitored threshold get no `δ`.
//!
//! [`epsilon_sample_floor`]: (mirrors `data_plane::monitor::sampling_alloc::epsilon_sample_floor`;
//! control_plane has no dependency on data_plane, so the canonical one-liner is
//! re-stated here — keep the two in sync.)

use crate::query_planning::QuerySetPlan;
use crate::types::SketchType;

/// Admission sampling probability under the unified ε-floor law,
/// `p = 1/(1 + ε²·rate)`.
///
/// `rate` is the per-window update count for the metric (NOT a probability).
/// Degenerate inputs (`rate ≤ 0` or `ε ≤ 0`) mean "no sampling" → `1.0`.
/// Mirrors `data_plane::monitor::sampling_alloc::epsilon_sample_floor`.
pub fn derive_sample_p(epsilon: f64, rate: f64) -> f64 {
    if rate <= 0.0 || epsilon <= 0.0 {
        return 1.0;
    }
    1.0 / (1.0 + epsilon * epsilon * rate)
}

/// Per-site CDM delta-transmission slack, `δ = ε·τ / k`.
///
/// The global threshold `τ` is the monitored value of interest (a user/query
/// input). The even-split CDM construction gives each of `k` sites a local
/// slack whose deviations sum to at most the global `ε·τ` budget. `k` is
/// clamped to ≥ 1. (A rate-weighted split is a possible refinement; the even
/// split is the baseline CDM allocation.)
pub fn derive_delta_threshold(epsilon: f64, tau: f64, n_sites: u32) -> f64 {
    let k = n_sites.max(1) as f64;
    epsilon * tau / k
}

/// The runtime knobs derived for one metric.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricKnobs {
    /// Admission sampling probability (ε-floor). `1.0` = no sampling.
    pub sample_p: f64,
    /// Per-site CDM delta-transmission slack, present only for metrics that
    /// carry a monitored threshold `τ`.
    pub delta_threshold: Option<f64>,
}

/// A fully-allocated metric: the sketches to stand up plus the runtime knobs.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocatedMetric {
    pub metric_name: String,
    pub sketches: Vec<SketchType>,
    pub knobs: MetricKnobs,
}

/// Attach ε-derived knobs to every metric in a [`QuerySetPlan`].
///
/// * `rate_for(metric)` → the metric's estimated per-window update rate (drives
///   `p`). At call sites this comes from runtime telemetry or a workload
///   estimate; kept as a closure so the deriver stays pure and testable.
/// * `monitor_for(metric)` → `Some((τ, k))` if the metric carries a CDM
///   threshold (drives `δ`), else `None` (no delta-transmission).
pub fn allocate_knobs(
    epsilon: f64,
    plan: &QuerySetPlan,
    mut rate_for: impl FnMut(&str) -> f64,
    mut monitor_for: impl FnMut(&str) -> Option<(f64, u32)>,
) -> Vec<AllocatedMetric> {
    plan.per_metric
        .iter()
        .map(|m| {
            let sample_p = derive_sample_p(epsilon, rate_for(&m.metric_name));
            let delta_threshold = monitor_for(&m.metric_name)
                .map(|(tau, k)| derive_delta_threshold(epsilon, tau, k));
            AllocatedMetric {
                metric_name: m.metric_name.clone(),
                sketches: m.sketches.clone(),
                knobs: MetricKnobs {
                    sample_p,
                    delta_threshold,
                },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planning::plan_sketches_for_queries;

    #[test]
    fn sample_p_follows_epsilon_floor() {
        // p = 1/(1+ε²·rate); ε=0.05, rate=1000 → 1/(1+2.5) = 0.2857…
        let p = derive_sample_p(0.05, 1000.0);
        assert!((p - 1.0 / 3.5).abs() < 1e-9, "got {p}");
    }

    #[test]
    fn sample_p_degenerate_inputs_disable_sampling() {
        assert_eq!(derive_sample_p(0.0, 1000.0), 1.0); // ε=0
        assert_eq!(derive_sample_p(0.05, 0.0), 1.0); // rate=0
    }

    #[test]
    fn sample_p_monotonic_decreasing_in_rate_and_epsilon() {
        assert!(derive_sample_p(0.05, 100.0) > derive_sample_p(0.05, 10_000.0));
        assert!(derive_sample_p(0.01, 1000.0) > derive_sample_p(0.10, 1000.0));
    }

    #[test]
    fn delta_threshold_is_even_split_of_epsilon_tau() {
        // ε·τ/k = 0.05 * 7000 / 5 = 70
        assert!((derive_delta_threshold(0.05, 7000.0, 5) - 70.0).abs() < 1e-9);
        // k clamps to ≥ 1
        assert!((derive_delta_threshold(0.05, 7000.0, 0) - 350.0).abs() < 1e-9);
    }

    #[test]
    fn allocate_knobs_attaches_p_and_optional_delta() {
        let plan = plan_sketches_for_queries([
            "quantile_over_time(0.99, latency_ms[5m])",
        ]);
        // latency_ms has a monitored threshold τ=7000 over k=4 sites; rate 2000/win
        let allocated = allocate_knobs(
            0.05,
            &plan,
            |_m| 2000.0,
            |m| if m == "latency_ms" { Some((7000.0, 4)) } else { None },
        );
        let a = allocated
            .iter()
            .find(|a| a.metric_name == "latency_ms")
            .expect("metric present");
        // p = 1/(1+0.0025*2000) = 1/6 = 0.1667…
        assert!((a.knobs.sample_p - 1.0 / 6.0).abs() < 1e-9, "p={}", a.knobs.sample_p);
        // δ = 0.05*7000/4 = 87.5
        assert_eq!(a.knobs.delta_threshold, Some(87.5));
        // the sketch set survived from slice 2
        assert!(
            a.sketches.contains(&SketchType::DDSketch) || a.sketches.contains(&SketchType::KLL)
        );
    }

    #[test]
    fn metric_without_monitor_gets_no_delta() {
        let plan = plan_sketches_for_queries([
            "quantile_over_time(0.99, latency_ms[5m])",
        ]);
        let allocated = allocate_knobs(0.05, &plan, |_m| 500.0, |_m| None);
        let a = &allocated[0];
        assert_eq!(a.knobs.delta_threshold, None);
        assert!(a.knobs.sample_p < 1.0); // sampling still applies
    }
}
