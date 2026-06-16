//! Distributed **L2 / second-frequency-moment (`F2 = ‖f‖₂²`) threshold monitoring**.
//!
//! The L1 coordinator (`coordinator.rs`) tracks an **additive** functional, so
//! the global value is `Σ known_value_i` — a linear merge. `F2` does **not**
//! merge linearly: for `f = Σ_i f_i`,
//!
//! ```text
//!   F2(f) = Σ_x (Σ_i f_i(x))²
//!         = Σ_i F2(f_i)  +  2 Σ_{i<j} ⟨f_i, f_j⟩      // cross terms!
//! ```
//!
//! so summing the per-edge `F2(f_i)` MISSES the `2⟨f_i,f_j⟩` cross terms. The
//! standard fix (Alon–Matias–Szegedy / Cormode–Garofalakis distributed F2) is to
//! keep a **linear sketch** at each edge: the AMS "tug-of-war" estimator
//! `Z = Σ_x g(x)·f(x)` with a ±1 hash `g`. `Z` is linear in the updates, so
//! `Z_merged = Σ_i Z_i`, and `E[Z_merged²] = F2(Σ_i f_i)` — squaring the MERGED
//! sketch recovers the cross terms automatically. Averaging `s1 = O(1/ε²)`
//! independent estimators bounds the variance; a median over `s2 = O(log 1/δ)`
//! groups bounds the failure probability.
//!
//! This module is pure (no I/O), `#[forbid(unsafe_code)]`-clean, and fully
//! unit-tested. It plugs into the CDM machinery the same way the L1 coordinator
//! does, but the per-edge "report" is the edge's AMS sketch (a small `i64`
//! vector) instead of a scalar `known_value`; the coordinator merges them and
//! tests `F2̂ ≥ (1−ε)·τ`.
//!
//! Coordinated sampling still applies unchanged: the per-edge sampling
//! probability is `p_i ∝ √(f_i/rate_i)` with the SAME per-key `f_i` as L1 (see
//! `sampling_alloc`); only the variance budget's norm differs — for L2 it is
//! `(ε·‖f‖₂)² = ε²·F2`, which [`DistributedF2Monitor::sampling_var_budget`]
//! computes from the live `F2̂`.

use std::collections::HashMap;

/// SplitMix64 finalizer — a cheap, well-mixed deterministic hash used to derive
/// the per-estimator ±1 sign for a key.
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EB);
    x ^= x >> 31;
    x
}

/// `g_i(key) ∈ {+1, −1}` for estimator `i` under `seed` — pairwise-independent
/// enough across (i, key) for `E[g_i(x)g_i(y)] = 0` (x≠y), which is what makes
/// `E[Z_i²] = F2` unbiased.
#[inline]
fn sign(seed: u64, idx: usize, key: u64) -> i64 {
    let h = mix64(
        seed ^ (idx as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ mix64(key),
    );
    if h & 1 == 0 {
        1
    } else {
        -1
    }
}

/// AMS tug-of-war F2 sketch: `s1·s2` linear estimators `Z[i] = Σ_x g_i(x)·f(x)`.
/// Linear in updates ⇒ mergeable; `mean_{group}(Z²)` estimates F2, `median`
/// across groups bounds failure probability.
#[derive(Clone, Debug)]
pub struct AmsF2Sketch {
    s1: usize, // averaging width per group  (~1/ε²)
    s2: usize, // median depth (# groups)    (~log 1/δ)
    seed: u64,
    z: Vec<i64>, // length s1*s2
}

impl AmsF2Sketch {
    pub fn new(s1: usize, s2: usize, seed: u64) -> Self {
        assert!(s1 > 0 && s2 > 0, "AMS dimensions must be positive");
        Self {
            s1,
            s2,
            seed,
            z: vec![0; s1 * s2],
        }
    }

    pub fn dims(&self) -> (usize, usize, u64) {
        (self.s1, self.s2, self.seed)
    }

    /// Add `delta` occurrences of `key` (delta may be negative).
    pub fn update(&mut self, key: u64, delta: i64) {
        for i in 0..self.z.len() {
            self.z[i] += sign(self.seed, i, key) * delta;
        }
    }

    /// Linearly merge another sketch (must share dimensions + seed) into this one.
    /// This is the step that makes distributed F2 correct: `Z_merged = Σ Z_i`,
    /// and the later square recovers the cross terms.
    pub fn merge(&mut self, other: &AmsF2Sketch) {
        assert_eq!(
            (self.s1, self.s2, self.seed),
            (other.s1, other.s2, other.seed),
            "cannot merge AMS sketches with different (s1, s2, seed)"
        );
        for i in 0..self.z.len() {
            self.z[i] += other.z[i];
        }
    }

    /// Unbiased F2 estimate: median over the `s2` groups of the per-group mean of
    /// `Z²` (each `Z²` is an unbiased F2 estimator; the mean shrinks variance).
    pub fn estimate_f2(&self) -> f64 {
        let mut group_means: Vec<f64> = Vec::with_capacity(self.s2);
        for g in 0..self.s2 {
            let mut sum = 0.0;
            for j in 0..self.s1 {
                let z = self.z[g * self.s1 + j] as f64;
                sum += z * z;
            }
            group_means.push(sum / self.s1 as f64);
        }
        median(&mut group_means)
    }
}

fn median(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        0.5 * (v[n / 2 - 1] + v[n / 2])
    }
}

/// Result of an L2 monitor report ingestion.
#[derive(Clone, Debug, PartialEq)]
pub enum F2Action {
    /// `F2̂` is comfortably below the threshold — nothing to do.
    Ok { f2_estimate: f64 },
    /// `F2̂ ≥ (1−ε)·τ`: the monitored second moment crossed the threshold band.
    Alert { f2_estimate: f64, tau: f64 },
}

/// Distributed F2 threshold monitor: holds each edge's latest AMS sketch, merges
/// them on every report, estimates the global `F2`, and fires once per epoch
/// when `F2̂ ≥ (1−ε)·τ`. `τ` is a threshold on `F2 = ‖f‖₂²`.
pub struct DistributedF2Monitor {
    tau: f64,
    epsilon: f64,
    s1: usize,
    s2: usize,
    seed: u64,
    window_start_ms: u64,
    edges: HashMap<String, AmsF2Sketch>,
    alerted: bool,
}

impl DistributedF2Monitor {
    pub fn new(tau: f64, epsilon: f64, s1: usize, s2: usize, seed: u64) -> Self {
        Self {
            tau,
            epsilon,
            s1,
            s2,
            seed,
            window_start_ms: 0,
            edges: HashMap::new(),
            alerted: false,
        }
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// A fresh, correctly-dimensioned sketch an edge (or a test) can fill and
    /// report — guarantees the (s1, s2, seed) match so merges never panic.
    pub fn new_edge_sketch(&self) -> AmsF2Sketch {
        AmsF2Sketch::new(self.s1, self.s2, self.seed)
    }

    fn ensure_epoch(&mut self, window_start_ms: u64) -> bool {
        if window_start_ms == self.window_start_ms {
            return true;
        }
        if window_start_ms > self.window_start_ms {
            self.window_start_ms = window_start_ms;
            self.edges.clear();
            self.alerted = false;
            return true;
        }
        false // stale epoch
    }

    /// Ingest one edge's AMS sketch for `window_start_ms`. Replaces that edge's
    /// prior sketch (the edge reports its running window sketch), re-estimates
    /// the merged global F2, and returns an Alert iff it crossed `(1−ε)τ`.
    pub fn on_report(
        &mut self,
        edge_id: &str,
        window_start_ms: u64,
        sketch: AmsF2Sketch,
    ) -> Option<F2Action> {
        if sketch.dims() != (self.s1, self.s2, self.seed) {
            return None; // mis-dimensioned report — drop (caller should log)
        }
        if !self.ensure_epoch(window_start_ms) {
            return None; // stale epoch
        }
        self.edges.insert(edge_id.to_string(), sketch);
        let f2 = self.global_f2();
        if !self.alerted && f2 >= (1.0 - self.epsilon) * self.tau {
            self.alerted = true;
            return Some(F2Action::Alert {
                f2_estimate: f2,
                tau: self.tau,
            });
        }
        Some(F2Action::Ok { f2_estimate: f2 })
    }

    /// Current global `F2̂` = estimate from the linear merge of all edge sketches.
    /// The merge is what captures the cross terms `2⟨f_i,f_j⟩` a per-edge
    /// `Σ F2(f_i)` would miss.
    pub fn global_f2(&self) -> f64 {
        let mut merged = AmsF2Sketch::new(self.s1, self.s2, self.seed);
        for s in self.edges.values() {
            merged.merge(s);
        }
        merged.estimate_f2()
    }

    /// L2 variance budget for the coordinated-sampling allocation:
    /// `V = (ε·‖f‖₂)² = ε²·F2`. Feed this (with the SAME per-key `f_i`/`rate_i`
    /// as L1) into `sampling_alloc::allocate_sample_rates` to size `p_i` for an
    /// L2-monitored quantity. Unlike L1's `(ε·τ)²` this tracks the LIVE L2 mass.
    pub fn sampling_var_budget(&self) -> f64 {
        let eps = self.epsilon;
        eps * eps * self.global_f2()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Exact F2 of a sparse vector given as (key, freq) pairs.
    fn true_f2(v: &[(u64, i64)]) -> f64 {
        v.iter().map(|&(_, f)| (f as f64) * (f as f64)).sum()
    }

    fn fill(sk: &mut AmsF2Sketch, v: &[(u64, i64)]) {
        for &(k, f) in v {
            sk.update(k, f);
        }
    }

    #[test]
    fn ams_estimates_f2_within_tolerance() {
        // 200 keys with freq = key  ⇒  F2 = Σ k² for k=1..200.
        let v: Vec<(u64, i64)> = (1..=200u64).map(|k| (k, k as i64)).collect();
        let exact = true_f2(&v);
        let mut sk = AmsF2Sketch::new(1024, 9, 0xABCD_1234);
        fill(&mut sk, &v);
        let est = sk.estimate_f2();
        let rel = (est - exact).abs() / exact;
        assert!(rel < 0.12, "F2 est {est} vs exact {exact} (rel {rel})");
    }

    #[test]
    fn merge_captures_cross_terms() {
        // a = {1:10, 2:10}; b = {1:10, 3:10}. They SHARE key 1.
        //   F2(a) = 200, F2(b) = 200, Σ local = 400.
        //   merged f = {1:20, 2:10, 3:10}  ⇒  F2 = 400 + 100 + 100 = 600.
        // A linear-merge sketch must estimate ~600 (NOT 400) — the cross term
        // 2·⟨a,b⟩ = 2·(10·10) = 200 is exactly the gap. This is the whole point.
        let a = [(1u64, 10i64), (2, 10)];
        let b = [(1u64, 10i64), (3, 10)];
        let sum_local = true_f2(&a) + true_f2(&b); // 400
        let merged_exact = 600.0;
        let seed = 0x5151_2727;
        let mut sa = AmsF2Sketch::new(1024, 9, seed);
        let mut sb = AmsF2Sketch::new(1024, 9, seed);
        fill(&mut sa, &a);
        fill(&mut sb, &b);
        sa.merge(&sb);
        let est = sa.estimate_f2();
        assert!(
            (est - merged_exact).abs() / merged_exact < 0.15,
            "merged F2 est {est} should track {merged_exact} (cross terms), not {sum_local}"
        );
        assert!(
            est > 0.5 * (sum_local + merged_exact),
            "est {est} must clearly exceed the cross-term-free sum {sum_local}"
        );
    }

    #[test]
    fn monitor_alert_threshold_behaviour() {
        let seed = 0x2468_1357;
        let eps = 0.1;
        // Build the merged-F2 ≈ 600 scenario across two edges.
        let edge_a = [(1u64, 10i64), (2, 10)];
        let edge_b = [(1u64, 10i64), (3, 10)];

        // tau well ABOVE 600 ⇒ no alert.
        let mut hi = DistributedF2Monitor::new(2_000.0, eps, 1024, 9, seed);
        let mut a1 = hi.new_edge_sketch();
        let mut b1 = hi.new_edge_sketch();
        fill(&mut a1, &edge_a);
        fill(&mut b1, &edge_b);
        hi.on_report("a", 0, a1);
        let r_hi = hi.on_report("b", 0, b1).unwrap();
        assert!(matches!(r_hi, F2Action::Ok { .. }), "below tau ⇒ Ok, got {r_hi:?}");

        // tau BELOW 600 ⇒ alert once.
        let mut lo = DistributedF2Monitor::new(400.0, eps, 1024, 9, seed);
        let mut a2 = lo.new_edge_sketch();
        let mut b2 = lo.new_edge_sketch();
        fill(&mut a2, &edge_a);
        fill(&mut b2, &edge_b);
        lo.on_report("a", 0, a2);
        let r_lo = lo.on_report("b", 0, b2).unwrap();
        assert!(matches!(r_lo, F2Action::Alert { .. }), "above tau ⇒ Alert, got {r_lo:?}");
    }

    #[test]
    fn sampling_var_budget_is_eps2_times_f2() {
        let seed = 0x1357_2468;
        let eps = 0.2;
        let mut mon = DistributedF2Monitor::new(10_000.0, eps, 1024, 9, seed);
        let mut e = mon.new_edge_sketch();
        fill(&mut e, &(1..=100u64).map(|k| (k, k as i64)).collect::<Vec<_>>());
        mon.on_report("e", 0, e);
        let f2 = mon.global_f2();
        let v = mon.sampling_var_budget();
        assert!((v - eps * eps * f2).abs() < 1e-6, "var_budget {v} != eps^2*F2 {}", eps * eps * f2);
        assert!(v > 0.0);
    }

    #[test]
    fn epoch_advance_clears_edges() {
        let mut mon = DistributedF2Monitor::new(1_000.0, 0.1, 256, 5, 7);
        let mut e = mon.new_edge_sketch();
        e.update(1, 5);
        mon.on_report("e", 0, e);
        assert_eq!(mon.edge_count(), 1);
        // New epoch resets membership.
        let e2 = mon.new_edge_sketch();
        mon.on_report("e", 60_000, e2);
        assert_eq!(mon.edge_count(), 1);
        // A stale-epoch report is dropped.
        let e3 = mon.new_edge_sketch();
        assert_eq!(mon.on_report("e", 0, e3), None);
    }
}
