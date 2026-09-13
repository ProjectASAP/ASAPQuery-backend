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


/// GOS budget split (design §7, Layer B) — **exact linear peel**.
///
/// The staleness term `ε_st` is a *deterministic worst-case* bound, so it adds
/// **linearly** to the random error (Theorem 1): the composition is
/// `√(ε_sk² + ε_sa²) + ε_st ≤ ε_q`, NOT a three-way quadrature. This deriver
/// therefore peels `ε_st` off linearly first and splits only the *random*
/// residual between the sketch and sampling:
///
/// 1. linear headroom above the sketch: `excess = ε_q − ε_sk` (0 if the sketch
///    already consumes the budget);
/// 2. give a fraction `t = w_comm/(w_edge+w_comm)` of it to staleness:
///    `ε_st = t·excess` (more comm-weight ⇒ looser thresholds ⇒ larger `ε_st`);
/// 3. the remaining *random* budget is `ε_rand = ε_q − ε_st`, split in
///    quadrature: `ε_sa = √(ε_rand² − ε_sk²)`.
///
/// This satisfies `√(ε_sk² + ε_sa²) + ε_st = ε_q` **exactly**. Limits:
///   * `w_edge = 0` ⇒ `t=1` ⇒ `ε_st = ε_q−ε_sk`, `ε_rand=ε_sk`, `ε_sa=0` ⇒
///     **no sampling** (`p=1`), cheap communication;
///   * `w_comm = 0` ⇒ `t=0` ⇒ `ε_st=0`, `ε_sa=√(ε_q²−ε_sk²)` ⇒ coarse sampling,
///     tight thresholds.
pub fn split_budget(eps_total: f64, eps_sketch: f64, w_edge: f64, w_comm: f64) -> (f64, f64) {
    if eps_total <= eps_sketch {
        return (0.0, 0.0); // the sketch already uses the whole ε budget
    }
    let excess = eps_total - eps_sketch; // linear headroom above the sketch
    let t = if w_edge + w_comm <= 0.0 {
        0.0
    } else {
        w_comm / (w_edge + w_comm)
    };
    let eps_st = t * excess;
    let eps_rand = eps_total - eps_st; // random budget after the linear peel
    let eps_sa = (eps_rand * eps_rand - eps_sketch * eps_sketch)
        .max(0.0)
        .sqrt();
    (eps_sa, eps_st)
}










#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_p_follows_epsilon_floor() {
        // p = 1/(1+ε²·rate); ε=0.05, rate=1000 → 1/(1+2.5) = 0.2857…
        let p = derive_sample_p(0.05, 1000.0);
        assert!((p - 1.0 / 3.5).abs() < 1e-9, "got {p}");
    }

    #[test]
    fn split_budget_linear_peel_and_limits() {
        // Exact linear composition: √(ε_sk² + ε_sa²) + ε_st = ε_q.
        let (eq, esk) = (0.1, 0.03);
        let (sa, st) = split_budget(eq, esk, 1.0, 1.0);
        assert!(((esk * esk + sa * sa).sqrt() + st - eq).abs() < 1e-12);
        // No edge-CPU concern ⇒ no sampling (ε_sa=0), all headroom to staleness.
        let (sa, st) = split_budget(eq, esk, 0.0, 1.0);
        assert!(sa.abs() < 1e-12);
        assert!((st - (eq - esk)).abs() < 1e-12);
        // No comm concern ⇒ no staleness, ε_sa = √(ε_q²−ε_sk²).
        let (sa, st) = split_budget(eq, esk, 1.0, 0.0);
        assert_eq!(st, 0.0);
        assert!((sa - (eq * eq - esk * esk).sqrt()).abs() < 1e-12);
        // Higher edge weight ⇒ more of the budget to sampling (larger ε_sa).
        let (sa_hi, _) = split_budget(eq, esk, 4.0, 1.0);
        let (sa_lo, _) = split_budget(eq, esk, 1.0, 1.0);
        assert!(sa_hi > sa_lo);
        // Sketch consumes the whole budget ⇒ nothing left.
        assert_eq!(split_budget(0.03, 0.05, 1.0, 1.0), (0.0, 0.0));
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






}
