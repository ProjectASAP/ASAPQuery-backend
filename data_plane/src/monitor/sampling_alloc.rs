//! Coordinated update-sampling: the per-edge sampling-probability `p_i` law.
//!
//! For a SKETCH-based warm tier the law is a single, closed form — the
//! **whole-sketch ε-floor**
//!
//! ```text
//!   p_i = 1 / (1 + ε²·rate_i)
//! ```
//!
//! and that is all this module provides ([`epsilon_sample_floor`]).
//!
//! Why one law (and not a per-key `√(f_i/rate_i)` allocation): the sampling
//! protects the warm sketch's accuracy, and a sketch's point/L2 estimate error
//! is bounded by the sketch NORM (‖f‖, the rate spread over buckets —
//! NitroSketch), never by a single key's `f(x)`. So the only accuracy a per-edge
//! `p_i` buys is "keep this edge's L2/mass contribution within ε":
//! `ε_s = √((1−p)/(p·rate)) ≤ ε`, which solves to the floor above. The
//! `√(f_i/rate_i)` KKT allocation (minimise `Σ rate_i·p_i` s.t.
//! `Σ f_i·(1−p_i)/p_i ≤ V`) only holds when a key is EXACT-counted OUTSIDE the
//! sketch — and sampling a single exact counter is pointless — so it is not a
//! valid sketch-sampling regime and has been retired.
//!
//! **Which sketches:** the law applies to any additive/linear sketch whose
//! estimate is unbiased (after a 1/p rescale) under update-sampling, and whose
//! error is norm- or rank-bounded: **Count-Min (L1), Count-Sketch (L2),
//! DDSketch & KLL (quantile rank; rank-preserving, no 1/p rescale needed), Sum**.
//! It does NOT apply to **HLL / cardinality**: update-sampling systematically
//! under-counts distinct items (you measure the sample's cardinality, not the
//! full set) and is not 1/p-correctable, so HLL needs a separate treatment.

/// The whole-sketch coordinated-sampling probability for an edge with the given
/// per-window `rate` (items/window): `p = 1/(1 + ε²·rate)`.
///
/// Derivation — never sample so hard that the per-edge sampling noise
/// `ε_s = √((1−p)/(p·rate))` exceeds the CDM threshold tolerance `ε`:
///
/// ```text
///   ε²·p·rate ≥ 1 − p   ⇒   p·(ε²·rate + 1) ≥ 1   ⇒   p ≥ 1/(1 + ε²·rate)
/// ```
///
/// With `rate ≤ 0` or `epsilon ≤ 0` the result is 1.0 (no sampling). The value is
/// always in `(0,1]` and grows toward 1 as the edge's rate falls (a low-`N` edge
/// can't absorb sampling noise within the band).
pub fn epsilon_sample_floor(epsilon: f64, rate: f64) -> f64 {
    if rate <= 0.0 || epsilon <= 0.0 {
        return 1.0;
    }
    1.0 / (1.0 + epsilon * epsilon * rate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epsilon_floor_monotone_and_bounded() {
        // Floor in (0,1], rises toward 1 as rate falls.
        let eps = 0.05;
        let hot = epsilon_sample_floor(eps, 100000.0);
        let quiet = epsilon_sample_floor(eps, 1000.0);
        assert!(hot > 0.0 && hot <= 1.0);
        assert!(quiet > 0.0 && quiet <= 1.0);
        assert!(quiet > hot, "lower-rate edge needs a higher floor");
        // Closed form: p = 1/(1 + ε²·rate).
        assert!((hot - 1.0 / (1.0 + eps * eps * 100000.0)).abs() < 1e-12);
        // Degenerate inputs ⇒ no sampling.
        assert_eq!(epsilon_sample_floor(eps, 0.0), 1.0);
        assert_eq!(epsilon_sample_floor(0.0, 1000.0), 1.0);
    }

    #[test]
    fn floor_keeps_sampling_noise_within_epsilon() {
        // The floored p keeps ε_s = √((1−p)/(p·rate)) ≤ ε.
        let eps = 0.05;
        let rate = 100000.0;
        let p = epsilon_sample_floor(eps, rate);
        let eps_s = ((1.0 - p) / (p * rate)).sqrt();
        assert!(eps_s <= eps + 1e-9, "ε_s {eps_s} should be ≤ ε {eps}");
    }
}
