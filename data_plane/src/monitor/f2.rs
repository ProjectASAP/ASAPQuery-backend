//! Distributed **L2 / second-frequency-moment (`F2 = ‖f‖₂²`) threshold monitoring**
//! over linearly-mergeable **Count-Sketch**es.
//!
//! `F2` does **not** merge linearly across edges — for `f = Σ_i f_i`,
//! ```text
//!   F2(f) = Σ_i F2(f_i)  +  2 Σ_{i<j} ⟨f_i, f_j⟩      // cross terms
//! ```
//! so summing per-edge `F2(f_i)` (or per-edge scalar L2) MISSES the
//! `2⟨f_i,f_j⟩` mass. The fix is to keep a **linear sketch** per edge and merge
//! the *sketches*, then estimate: a Count-Sketch row `C[r][b] = Σ_{x:h_r(x)=b}
//! s_r(x) f(x)` is linear in updates, so `C_merged = Σ_i C_i` and
//! `Σ_b C_merged[r][b]²` is an unbiased estimator of `F2(Σ_i f_i)` — squaring the
//! MERGED sketch recovers the cross terms automatically (the ±1 sign hash makes
//! the off-diagonal collisions zero-mean).
//!
//! Estimator: per row `r`, `F2̂_r = Σ_b C[r][b]²` (unbiased, variance `O(F2²/w)`);
//! `F2̂ = median_r F2̂_r` over the `d` rows (depth bounds the failure prob).
//! Width `w = O(1/ε²)`, depth `d = O(log 1/δ)`.
//!
//! Hashing is an engineering approximation: a murmur3 `fmix64` finalizer with a
//! **different seed per row, and a different salt for the bucket vs the sign**
//! (not provably 4-wise independent, but standard and fast — like xxhash/murmur).
//!
//! Division of labour:
//!   * **edge** maintains its local Count-Sketch `C_i` (shared seeds!) over `f_i`,
//!     and per window ships `(C_i, rate_i)` to the coordinator.
//!   * **coordinator** ([`DistributedF2Monitor`]) merges the `C_i` linearly,
//!     estimates the global `F2̂`, fires at `F2̂ ≥ (1−ε)τ`, and allocates the
//!     coordinated sampling `p_i ∝ √(F2̂_i / rate_i)` (local F2 as the freq
//!     weight; `var_budget = (ε·‖f‖₂)² = ε²·F2̂`).

use std::collections::HashMap;

use super::sampling_alloc::{allocate_sample_rates, epsilon_sample_floor};

/// murmur3 64-bit finalizer (`fmix64`).
#[inline]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

const SALT_BUCKET: u64 = 0x9E37_79B9_7F4A_7C15;
const SALT_SIGN: u64 = 0xC2B2_AE3D_27D4_EB4F;

/// Per-row bucket index `h_r(key) ∈ [0, w)`.
#[inline]
fn bucket(seed: u64, row: usize, key: u64, w: usize) -> usize {
    let row_seed = fmix64(seed ^ SALT_BUCKET ^ (row as u64).wrapping_mul(SALT_BUCKET));
    (fmix64(row_seed ^ key) % w as u64) as usize
}

/// Per-row sign `s_r(key) ∈ {+1, −1}` (independent salt from the bucket hash).
#[inline]
fn sign(seed: u64, row: usize, key: u64) -> i64 {
    let row_seed = fmix64(seed ^ SALT_SIGN ^ (row as u64).wrapping_mul(SALT_SIGN));
    if fmix64(row_seed ^ key) & 1 == 0 {
        1
    } else {
        -1
    }
}

/// Count-Sketch: `d` rows × `w` buckets, signed. Linear in updates ⇒ mergeable;
/// `median_r Σ_b C[r][b]²` estimates `F2 = ‖f‖₂²`.
#[derive(Clone, Debug)]
pub struct CountSketchF2 {
    d: usize, // depth (rows / median groups)  ~ log(1/δ)
    w: usize, // width (buckets per row)        ~ 1/ε²
    seed: u64,
    c: Vec<i64>, // length d*w, row-major
}

impl CountSketchF2 {
    pub fn new(d: usize, w: usize, seed: u64) -> Self {
        assert!(d > 0 && w > 0, "Count-Sketch dimensions must be positive");
        Self {
            d,
            w,
            seed,
            c: vec![0; d * w],
        }
    }

    pub fn dims(&self) -> (usize, usize, u64) {
        (self.d, self.w, self.seed)
    }

    /// Add `delta` occurrences of `key` (delta may be negative / fractional via
    /// pre-scaled integer counts).
    pub fn update(&mut self, key: u64, delta: i64) {
        for r in 0..self.d {
            let b = bucket(self.seed, r, key, self.w);
            self.c[r * self.w + b] += sign(self.seed, r, key) * delta;
        }
    }

    /// Linearly merge another sketch (must share `(d, w, seed)`). THIS is what
    /// makes distributed F2 correct: `C_merged = Σ_i C_i`, and the later square
    /// recovers the cross terms a per-edge `Σ F2(f_i)` would miss.
    pub fn merge(&mut self, other: &CountSketchF2) {
        assert_eq!(
            (self.d, self.w, self.seed),
            (other.d, other.w, other.seed),
            "cannot merge Count-Sketches with different (d, w, seed)"
        );
        for i in 0..self.c.len() {
            self.c[i] += other.c[i];
        }
    }

    /// Unbiased F2 estimate: median over the `d` rows of `Σ_b C[r][b]²`.
    pub fn estimate_f2(&self) -> f64 {
        let mut row_f2: Vec<f64> = Vec::with_capacity(self.d);
        for r in 0..self.d {
            let mut s = 0.0f64;
            for b in 0..self.w {
                let v = self.c[r * self.w + b] as f64;
                s += v * v;
            }
            row_f2.push(s);
        }
        median(&mut row_f2)
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

/// Result of ingesting one edge's Count-Sketch report.
#[derive(Clone, Debug, PartialEq)]
pub enum F2Action {
    Ok { f2_estimate: f64 },
    Alert { f2_estimate: f64, tau: f64 },
}

#[derive(Clone, Debug)]
struct EdgeF2 {
    sketch: CountSketchF2,
    rate: f64,
}

/// Distributed F2 threshold monitor + coordinated-sampling allocator. Holds each
/// edge's latest Count-Sketch + rate, merges on report, estimates the global
/// `F2̂`, fires at `F2̂ ≥ (1−ε)τ`, and allocates `p_i ∝ √(F2̂_i / rate_i)`.
pub struct DistributedF2Monitor {
    tau: f64,
    epsilon: f64,
    d: usize,
    w: usize,
    seed: u64,
    window_start_ms: u64,
    edges: HashMap<String, EdgeF2>,
    alerted: bool,
}

impl DistributedF2Monitor {
    pub fn new(tau: f64, epsilon: f64, d: usize, w: usize, seed: u64) -> Self {
        Self {
            tau,
            epsilon,
            d,
            w,
            seed,
            window_start_ms: 0,
            edges: HashMap::new(),
            alerted: false,
        }
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// A correctly-dimensioned empty sketch for an edge to fill (guarantees the
    /// (d, w, seed) match so merges never panic).
    pub fn new_edge_sketch(&self) -> CountSketchF2 {
        CountSketchF2::new(self.d, self.w, self.seed)
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
        false
    }

    /// Ingest one edge's `(Count-Sketch, rate)` for `window_start_ms`. Replaces
    /// that edge's prior report, re-estimates the merged global F2, and returns
    /// an Alert iff it crossed `(1−ε)τ`.
    pub fn on_report(
        &mut self,
        edge_id: &str,
        window_start_ms: u64,
        sketch: CountSketchF2,
        rate: f64,
    ) -> Option<F2Action> {
        if sketch.dims() != (self.d, self.w, self.seed) {
            return None; // mis-dimensioned — drop
        }
        if !self.ensure_epoch(window_start_ms) {
            return None; // stale epoch
        }
        self.edges.insert(edge_id.to_string(), EdgeF2 { sketch, rate });
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

    /// Global `F2̂` from the linear merge of all edge Count-Sketches (captures the
    /// cross terms a per-edge `Σ F2(f_i)` would miss).
    pub fn global_f2(&self) -> f64 {
        let mut merged = CountSketchF2::new(self.d, self.w, self.seed);
        for e in self.edges.values() {
            merged.merge(&e.sketch);
        }
        merged.estimate_f2()
    }

    /// L2 sampling variance budget `V = (ε·‖f‖₂)² = ε²·F2̂` (vs L1's `(ε·τ)²`).
    pub fn sampling_var_budget(&self) -> f64 {
        self.epsilon * self.epsilon * self.global_f2()
    }

    /// Coordinated per-edge sampling probability `p_i ∝ √(F2̂_i / rate_i)`: the
    /// freq weight is each edge's LOCAL F2 (from its own sketch), the rate is its
    /// reported update rate, the budget is `ε²·F2̂_global`. An edge contributing
    /// more L2 mass is sampled less (higher p) to preserve the F2 estimate.
    /// Mirrors the L1 `coordinator::allocate_p` but with L2 freq/budget.
    pub fn allocate_p(&self) -> HashMap<String, f64> {
        let ids: Vec<&String> = self.edges.keys().collect();
        let rates: Vec<f64> = ids.iter().map(|id| self.edges[*id].rate).collect();
        let freqs: Vec<f64> = ids
            .iter()
            .map(|id| self.edges[*id].sketch.estimate_f2()) // local F2̂_i
            .collect();
        let var_budget = self.sampling_var_budget();
        let mut p = allocate_sample_rates(&rates, &freqs, var_budget);
        for (i, &rate) in rates.iter().enumerate() {
            let floor = epsilon_sample_floor(self.epsilon, rate);
            if p[i] < floor {
                p[i] = floor;
            }
        }
        ids.into_iter()
            .cloned()
            .zip(p)
            .map(|(id, pi)| (id, if pi > 0.0 && pi <= 1.0 { pi } else { 1.0 }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn true_f2(v: &[(u64, i64)]) -> f64 {
        v.iter().map(|&(_, f)| (f as f64) * (f as f64)).sum()
    }
    fn fill(sk: &mut CountSketchF2, v: &[(u64, i64)]) {
        for &(k, f) in v {
            sk.update(k, f);
        }
    }

    #[test]
    fn count_sketch_estimates_f2_within_tolerance() {
        let v: Vec<(u64, i64)> = (1..=200u64).map(|k| (k, k as i64)).collect();
        let exact = true_f2(&v);
        let mut sk = CountSketchF2::new(9, 4096, 0xABCD_1234);
        fill(&mut sk, &v);
        let est = sk.estimate_f2();
        let rel = (est - exact).abs() / exact;
        assert!(rel < 0.12, "F2 est {est} vs exact {exact} (rel {rel})");
    }

    #[test]
    fn merge_captures_cross_terms() {
        // a={1:10,2:10}, b={1:10,3:10} share key 1. Σ local F2 = 400, but merged
        // f={1:20,2:10,3:10} ⇒ F2 = 600. The linear merge must estimate ~600.
        let a = [(1u64, 10i64), (2, 10)];
        let b = [(1u64, 10i64), (3, 10)];
        let sum_local = true_f2(&a) + true_f2(&b); // 400
        let merged_exact = 600.0;
        let seed = 0x5151_2727;
        let mut sa = CountSketchF2::new(9, 4096, seed);
        let mut sb = CountSketchF2::new(9, 4096, seed);
        fill(&mut sa, &a);
        fill(&mut sb, &b);
        sa.merge(&sb);
        let est = sa.estimate_f2();
        assert!(
            (est - merged_exact).abs() / merged_exact < 0.1,
            "merged F2 {est} should track {merged_exact} (cross terms), not {sum_local}"
        );
        assert!(est > 0.5 * (sum_local + merged_exact));
    }

    #[test]
    fn monitor_alert_threshold_behaviour() {
        let seed = 0x2468_1357;
        let eps = 0.1;
        let edge_a = [(1u64, 10i64), (2, 10)];
        let edge_b = [(1u64, 10i64), (3, 10)]; // merged F2 ≈ 600

        let mut hi = DistributedF2Monitor::new(2_000.0, eps, 9, 4096, seed);
        let mut a1 = hi.new_edge_sketch();
        let mut b1 = hi.new_edge_sketch();
        fill(&mut a1, &edge_a);
        fill(&mut b1, &edge_b);
        hi.on_report("a", 0, a1, 1000.0);
        let r_hi = hi.on_report("b", 0, b1, 1000.0).unwrap();
        assert!(matches!(r_hi, F2Action::Ok { .. }), "below tau ⇒ Ok, got {r_hi:?}");

        let mut lo = DistributedF2Monitor::new(400.0, eps, 9, 4096, seed);
        let mut a2 = lo.new_edge_sketch();
        let mut b2 = lo.new_edge_sketch();
        fill(&mut a2, &edge_a);
        fill(&mut b2, &edge_b);
        lo.on_report("a", 0, a2, 1000.0);
        let r_lo = lo.on_report("b", 0, b2, 1000.0).unwrap();
        assert!(matches!(r_lo, F2Action::Alert { .. }), "above tau ⇒ Alert, got {r_lo:?}");
    }

    #[test]
    fn allocate_p_higher_local_f2_keeps_higher_p_at_equal_rate() {
        // Equal rate; edge A carries far more L2 mass than edge B (disjoint keys).
        // p_i ∝ √(F2_i/rate) ⇒ A (high F2) keeps a HIGHER p (sampled less) to
        // preserve the F2 estimate. High rate ⇒ low floor so the allocation, not
        // the floor, drives the spread.
        let seed = 0x1357_2468;
        let mut mon = DistributedF2Monitor::new(1.0e12, 0.1, 9, 4096, seed);
        let mut a = mon.new_edge_sketch();
        let mut b = mon.new_edge_sketch();
        fill(&mut a, &[(1u64, 100i64)]); // local F2 ≈ 10000
        fill(&mut b, &[(2u64, 1i64)]); //  local F2 ≈ 1
        mon.on_report("a", 0, a, 1_000_000.0);
        mon.on_report("b", 0, b, 1_000_000.0);
        let p = mon.allocate_p();
        let pa = p["a"];
        let pb = p["b"];
        let floor = epsilon_sample_floor(0.1, 1_000_000.0);
        assert!(pa > pb, "high-F2 edge p {pa} should exceed low-F2 p {pb}");
        assert!(pa > floor + 1e-9 && pb > floor + 1e-9, "p ({pa},{pb}) above floor {floor}");
    }

    #[test]
    fn sampling_var_budget_is_eps2_times_f2() {
        let seed = 0x9999_1111;
        let eps = 0.2;
        let mut mon = DistributedF2Monitor::new(1.0e12, eps, 9, 4096, seed);
        let mut e = mon.new_edge_sketch();
        fill(&mut e, &(1..=100u64).map(|k| (k, k as i64)).collect::<Vec<_>>());
        mon.on_report("e", 0, e, 1000.0);
        let f2 = mon.global_f2();
        let v = mon.sampling_var_budget();
        assert!((v - eps * eps * f2).abs() < 1e-6);
        assert!(v > 0.0);
    }

    #[test]
    fn epoch_advance_clears_edges() {
        let mut mon = DistributedF2Monitor::new(1_000.0, 0.1, 5, 1024, 7);
        let mut e = mon.new_edge_sketch();
        e.update(1, 5);
        mon.on_report("e", 0, e, 10.0);
        assert_eq!(mon.edge_count(), 1);
        let e2 = mon.new_edge_sketch();
        mon.on_report("e", 60_000, e2, 10.0);
        assert_eq!(mon.edge_count(), 1);
        let e3 = mon.new_edge_sketch();
        assert_eq!(mon.on_report("e", 0, e3, 10.0), None); // stale
    }
}
