//! Distributed-NitroSketch coordinated update-sampling allocation — the
//! coordinator's per-edge sampling-probability `p_i` computation. Faithful Rust
//! port of the Go reference `asap-precompute-go/monitor/sampling_alloc.go`
//! (`AllocateSampleRates`), plus the CDM-threshold coupling floor from
//! `docs/distributed-nitrosketch-coordinated-sampling.md`
//! ("Combining with the CDM threshold").
//!
//! Each edge reports its observed per-window `rate` (items/window) over the
//! reverse `MonitorService` channel (`MonitorReport.rate`); the coordinator
//! holds the full per-edge rate vector for a monitor and allocates a `p_i` that
//! MINIMIZES total edge update work `Σ rate_i·p_i` subject to a merged
//! sampling-variance budget `V ≥ Σ f_i·(1−p_i)/p_i`. The KKT solution is
//!
//! ```text
//!   p_i  =  clamp( √λ · √(f_i / rate_i),  0, 1 )
//! ```
//!
//! i.e. sample harder (smaller `p`) on high-rate edges where local accuracy is
//! cheap, keep `p≈1` on low-rate edges. `√λ` is the single scale that makes the
//! variance constraint bind; solved by bisection (variance is monotone
//! decreasing in the scale). The result rides each edge's `SlackGrant.sample_p`.

/// Compute per-edge update-sampling probabilities `p_i`.
///
/// `rates[i]` = edge i's items/window; `freqs[i]` = edge i's mass of the queried
/// quantity (pass `rates` as a proxy when the per-key split is unknown — then
/// `p_i ∝ 1/√rate_i`). `var_budget` = `V` in the bound above. A `rate_i ≤ 0` or
/// `freqs_i ≤ 0` yields `p_i = 1` (nothing to sample).
pub fn allocate_sample_rates(rates: &[f64], freqs: &[f64], var_budget: f64) -> Vec<f64> {
    let n = rates.len();
    let mut p = vec![0.0_f64; n];
    if n == 0 {
        return p;
    }
    // base[i] = √(f_i/rate_i); 0 forces p_i = 1 (no rate ⇒ no sampling benefit).
    let mut base = vec![0.0_f64; n];
    let mut max_base = 0.0_f64;
    for i in 0..n {
        if rates[i] <= 0.0 || freqs[i] <= 0.0 {
            base[i] = 0.0;
            continue;
        }
        base[i] = (freqs[i] / rates[i]).sqrt();
        if base[i] > max_base {
            max_base = base[i];
        }
    }
    if max_base == 0.0 || var_budget <= 0.0 {
        return vec![1.0; n];
    }

    // variance(scale) = Σ f_i·(1−p_i)/p_i with p_i = min(1, scale·base_i).
    // Monotone decreasing in scale: scale→0 ⇒ p→0 ⇒ variance→∞; at
    // scale = 1/min(positive base_i) all p_i hit 1 ⇒ variance = 0.
    let variance = |scale: f64| -> f64 {
        let mut v = 0.0;
        for i in 0..n {
            if base[i] == 0.0 {
                continue; // p_i = 1, contributes 0
            }
            let pi = scale * base[i];
            if pi >= 1.0 {
                continue;
            }
            v += freqs[i] * (1.0 - pi) / pi;
        }
        v
    };

    // scale_hi: every positive-base edge clamped to p_i = 1 (variance 0 ≤ V).
    let mut min_base = f64::INFINITY;
    for &b in &base {
        if b > 0.0 && b < min_base {
            min_base = b;
        }
    }
    let scale_hi = 1.0 / min_base;
    if variance(scale_hi) >= var_budget {
        // Even no sampling (all p=1) can't meet V → don't sample at all.
        return vec![1.0; n];
    }
    // Bisect for the smallest scale (= most sampling, least CPU) with
    // variance(scale) ≤ var_budget.
    let mut lo = 0.0_f64;
    let mut hi = scale_hi;
    for _ in 0..100 {
        let mid = 0.5 * (lo + hi);
        if mid <= 0.0 {
            lo = mid;
            continue;
        }
        if variance(mid) > var_budget {
            lo = mid; // too much sampling (variance too high) → raise scale
        } else {
            hi = mid;
        }
    }
    let scale = hi;
    for i in 0..n {
        if base[i] == 0.0 {
            p[i] = 1.0;
        } else {
            p[i] = (scale * base[i]).min(1.0);
        }
    }
    p
}

/// The single uniform sampling probability `p` meeting the same variance budget
/// `V` with one rate everywhere (the per-edge-independent NitroSketch baseline):
/// `(1−p)/p·Σf = V ⇒ p = Σf/(Σf+V)`. Used to quantify the coordination win.
pub fn uniform_sample_rate(freqs: &[f64], var_budget: f64) -> f64 {
    let sum_f: f64 = freqs.iter().filter(|&&f| f > 0.0).sum();
    if sum_f <= 0.0 || var_budget <= 0.0 {
        return 1.0;
    }
    (sum_f / (sum_f + var_budget)).min(1.0)
}

/// The CDM-threshold coupling floor on `p_i`: never sample so hard that the
/// sampling noise `ε_s = √((1−p)/(p·rate))` exceeds the CDM threshold tolerance
/// `ε` (the agg's epsilon). Solving `ε ≥ √((1−p)/(p·rate))` for `p`:
///
/// ```text
///   ε²·p·rate ≥ 1 − p   ⇒   p·(ε²·rate + 1) ≥ 1   ⇒   p ≥ 1/(1 + ε²·rate)
/// ```
///
/// With `rate ≤ 0` or `epsilon ≤ 0` the floor is 1.0 (no sampling permitted). The
/// floor is always in `(0,1]` and grows toward 1 as the edge's rate falls (a
/// low-`N` edge can't absorb sampling noise within the band).
pub fn epsilon_sample_floor(epsilon: f64, rate: f64) -> f64 {
    if rate <= 0.0 || epsilon <= 0.0 {
        return 1.0;
    }
    1.0 / (1.0 + epsilon * epsilon * rate)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merged_variance(freqs: &[f64], p: &[f64]) -> f64 {
        let mut v = 0.0;
        for i in 0..freqs.len() {
            if p[i] > 0.0 && p[i] < 1.0 {
                v += freqs[i] * (1.0 - p[i]) / p[i];
            }
        }
        v
    }
    fn total_cpu(rates: &[f64], p: &[f64]) -> f64 {
        rates.iter().zip(p).map(|(r, p)| r * p).sum()
    }

    /// Mirrors Go `TestAllocateSampleRates_SkewedFleetBeatsUniform`: on a skewed
    /// fleet (one hot edge, a quiet tail) the coordinated p_i ∝ √(f_i/rate_i)
    /// allocation hits the same merged-variance budget at strictly LESS total
    /// edge update work than per-edge-uniform NitroSketch.
    #[test]
    fn skewed_fleet_beats_uniform() {
        let rates = [100000.0, 1000.0, 1000.0, 1000.0, 1000.0]; // 1 hot + 4 quiet
        let freqs = [100.0, 100.0, 100.0, 100.0, 100.0]; // key ~uniform across edges
        let v = 2000.0;

        let p_coord = allocate_sample_rates(&rates, &freqs, v);
        let p_uni = uniform_sample_rate(&freqs, v);
        let uni = vec![p_uni; rates.len()];

        let vc = merged_variance(&freqs, &p_coord);
        let vu = merged_variance(&freqs, &uni);
        assert!(vc <= v * 1.02, "coordinated variance {vc} exceeds budget {v}");
        assert!(
            (vu - v).abs() <= v * 0.02,
            "uniform variance {vu} should ≈ budget {v}"
        );
        let cc = total_cpu(&rates, &p_coord);
        let cu = total_cpu(&rates, &uni);
        assert!(
            cc < cu,
            "coordinated CPU {cc} should be < uniform CPU {cu} at equal variance"
        );
        // Hot edge sampled hard; quiet edges near p=1.
        assert!(
            p_coord[0] < p_coord[1],
            "hot edge should get smaller p than quiet edges: {p_coord:?}"
        );
    }

    /// Mirrors Go `TestAllocateSampleRates_FlatFleetNoWin`: on a uniform fleet
    /// the coordinated allocation collapses to ≈ the uniform rate (no win).
    #[test]
    fn flat_fleet_no_win() {
        let rates = [2000.0, 2000.0, 2000.0, 2000.0];
        let v = 3000.0;
        let p_coord = allocate_sample_rates(&rates, &rates, v);
        let p_uni = uniform_sample_rate(&rates, v);
        let cc = total_cpu(&rates, &p_coord);
        let cu = p_uni * (2000.0 * 4.0);
        assert!(
            (cc - cu).abs() <= 0.05 * cu,
            "flat fleet: coordinated CPU {cc} should ≈ uniform {cu} (no win)"
        );
    }

    #[test]
    fn zero_or_negative_rate_gets_no_sampling() {
        let rates = [0.0, -5.0, 1000.0];
        let freqs = [100.0, 100.0, 100.0];
        let p = allocate_sample_rates(&rates, &freqs, 50.0);
        assert_eq!(p[0], 1.0);
        assert_eq!(p[1], 1.0);
        assert!(p[2] <= 1.0 && p[2] > 0.0);
    }

    #[test]
    fn empty_input() {
        assert!(allocate_sample_rates(&[], &[], 100.0).is_empty());
    }

    #[test]
    fn zero_budget_means_no_sampling() {
        let rates = [1000.0, 2000.0];
        let p = allocate_sample_rates(&rates, &rates, 0.0);
        assert_eq!(p, vec![1.0, 1.0]);
    }

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
    fn floor_enforced_keeps_sampling_noise_within_epsilon() {
        // A floored p_i keeps ε_s = √((1−p)/(p·rate)) ≤ ε.
        let eps = 0.05;
        let rate = 100000.0;
        let floor = epsilon_sample_floor(eps, rate);
        let eps_s = ((1.0 - floor) / (floor * rate)).sqrt();
        assert!(eps_s <= eps + 1e-9, "ε_s {eps_s} should be ≤ ε {eps}");
    }
}
