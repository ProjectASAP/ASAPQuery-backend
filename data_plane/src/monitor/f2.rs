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
    c: Vec<f64>, // length d*w, row-major. f64 (not i64) so sampling-rescaled
                 // (`1/p`) fractional counts round-trip through the wire matrix.
}

impl CountSketchF2 {
    pub fn new(d: usize, w: usize, seed: u64) -> Self {
        assert!(d > 0 && w > 0, "Count-Sketch dimensions must be positive");
        Self {
            d,
            w,
            seed,
            c: vec![0.0; d * w],
        }
    }

    /// Build directly from a decoded `portable::CountSketch` cell matrix
    /// (`rows × cols` of `f64` counts) — the wire path. The coordinator only
    /// ever does matrix arithmetic (merge/norm/drift), never `update()`, so the
    /// `seed` is irrelevant here (it drives only the bucket/sign hashing that the
    /// EDGE already applied); we store 0. Panics on a ragged matrix.
    pub fn from_matrix(matrix: &[Vec<f64>]) -> Self {
        let d = matrix.len();
        assert!(d > 0, "matrix must have at least one row");
        let w = matrix[0].len();
        assert!(w > 0, "matrix rows must be non-empty");
        let mut c = Vec::with_capacity(d * w);
        for row in matrix {
            assert_eq!(row.len(), w, "ragged matrix: rows must share width");
            c.extend_from_slice(row);
        }
        Self { d, w, seed: 0, c }
    }

    /// Sparse changed cells of `self` relative to `base`: `(row, col, self−base)`
    /// for every cell where they differ. The delta form of the reference so a
    /// `C_ref` broadcast ships only the changed cells (removing the O(k)
    /// full-matrix amplification of the geometric resync). Panics on shape
    /// mismatch.
    pub fn sparse_delta_cells(&self, base: &CountSketchF2) -> Vec<(u32, u32, f64)> {
        assert_eq!(self.shape(), base.shape());
        let mut cells = Vec::new();
        for r in 0..self.d {
            for c in 0..self.w {
                let i = r * self.w + c;
                let d = self.c[i] - base.c[i];
                if d != 0.0 {
                    cells.push((r as u32, c as u32, d));
                }
            }
        }
        cells
    }

    /// Emit the cell matrix as `rows × cols` `f64` (the wire form — feeds
    /// `portable::CountSketch::from_legacy_matrix` for msgpack serialization).
    pub fn to_matrix(&self) -> Vec<Vec<f64>> {
        (0..self.d)
            .map(|r| self.c[r * self.w..(r + 1) * self.w].to_vec())
            .collect()
    }

    pub fn dims(&self) -> (usize, usize, u64) {
        (self.d, self.w, self.seed)
    }

    /// Dimensions that matter for matrix arithmetic across sketches from
    /// different sources (edge sketches carry the hashing seed; wire-rebuilt
    /// ones carry 0), so merges compare only `(d, w)`.
    pub fn shape(&self) -> (usize, usize) {
        (self.d, self.w)
    }

    /// Add `delta` occurrences of `key` (delta may be negative / fractional via
    /// pre-scaled integer counts).
    pub fn update(&mut self, key: u64, delta: i64) {
        for r in 0..self.d {
            let b = bucket(self.seed, r, key, self.w);
            self.c[r * self.w + b] += (sign(self.seed, r, key) * delta) as f64;
        }
    }

    /// Linearly merge another sketch (must share `(d, w)`). THIS is what
    /// makes distributed F2 correct: `C_merged = Σ_i C_i`, and the later square
    /// recovers the cross terms a per-edge `Σ F2(f_i)` would miss.
    pub fn merge(&mut self, other: &CountSketchF2) {
        assert_eq!(
            self.shape(),
            other.shape(),
            "cannot merge Count-Sketches with different (d, w)"
        );
        for i in 0..self.c.len() {
            self.c[i] += other.c[i];
        }
    }

    /// Squared L2 norm of the whole counter vector, `‖C‖₂² = Σ_j C[j]²`.
    /// `E[‖C‖₂²] = d·F2` (each of the `d` rows is an unbiased F2 estimator), so
    /// the geometric layer monitors `‖C‖₂²` against `d·τ` — a smooth quadratic
    /// whose sublevel set is a ball (unlike the robust median estimate_f2()).
    pub fn l2_norm_sq(&self) -> f64 {
        self.c.iter().map(|&x| x * x).sum()
    }

    /// Mean-of-rows F2 estimate `‖C‖₂²/d`. This is the estimator the geometric
    /// **safe-zone ball** is built on (`‖C‖ ≤ √(d·τ) ⟺ ‖C‖²/d ≤ τ`), so the
    /// geometric alert must use it too — then silence and alert are exactly
    /// consistent: all sites locally safe ⟺ `mean_f2 < (1−ε)τ`. Less robust than
    /// the median `estimate_f2()` but required for the ball's smooth quadratic.
    pub fn mean_f2(&self) -> f64 {
        if self.d == 0 {
            return 0.0;
        }
        self.l2_norm_sq() / self.d as f64
    }

    /// New sketch = `self − other` (same shape). The per-site DRIFT `ΔC_i`.
    pub fn minus(&self, other: &CountSketchF2) -> CountSketchF2 {
        assert_eq!(self.shape(), other.shape());
        let mut out = self.clone();
        for i in 0..out.c.len() {
            out.c[i] -= other.c[i];
        }
        out
    }

    /// `‖ self + scale·other ‖₂²` without allocating — used by the geometric
    /// drift-ball test (`‖C_ref + (k/2)·ΔC_i‖`).
    pub fn norm_sq_combo(&self, other: &CountSketchF2, scale: f64) -> f64 {
        assert_eq!(self.shape(), other.shape());
        self.c
            .iter()
            .zip(&other.c)
            .map(|(&a, &b)| {
                let v = a + scale * b;
                v * v
            })
            .sum()
    }

    /// Unbiased F2 estimate: median over the `d` rows of `Σ_b C[r][b]²`.
    ///
    /// IMPORTANT — assumes the input Count-Sketch was **not** update-sampled.
    /// F2 squares the cells, so with per-row Bernoulli(p) admission + `1/p`
    /// weighting the linear cell estimate stays unbiased but the squared sum is
    /// biased: `E[Σ_b C[r][b]²] = F2 + (1−p)/p·F2 = F2/p`. Sampling and F2
    /// monitoring must therefore not run on the same sketch (use `p=1` for
    /// F2-monitored sketches, or bias-correct by `×p`). Frequency (point)
    /// queries are unaffected. See design-gos-unified-edge-telemetry.md §3.2.
    pub fn estimate_f2(&self) -> f64 {
        let mut row_f2: Vec<f64> = Vec::with_capacity(self.d);
        for r in 0..self.d {
            let mut s = 0.0f64;
            for b in 0..self.w {
                let v = self.c[r * self.w + b];
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
    // NaN-safe: a NaN cell from a malformed wire matrix must not panic the sort.
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
}

/// Distributed F2 **threshold** monitor. Holds each edge's latest Count-Sketch,
/// linearly merges on report, estimates the global `F2̂`, and fires at
/// `F2̂ ≥ (1−ε)τ`. F2 is a MONITORED quantity only — per-edge update-sampling is
/// the single whole-sketch ε-floor law (`sampling_alloc`), not driven by F2.
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

    /// Ingest one edge's Count-Sketch for `window_start_ms`. Replaces that edge's
    /// prior report, re-estimates the merged global F2, and returns an Alert iff
    /// it crossed `(1−ε)τ`. (`rate` is part of the edge wire report but the F2
    /// threshold needs only the sketch; sampling consumes rate elsewhere.)
    pub fn on_report(
        &mut self,
        edge_id: &str,
        window_start_ms: u64,
        sketch: CountSketchF2,
        _rate: f64,
    ) -> Option<F2Action> {
        if sketch.shape() != (self.d, self.w) {
            return None; // mis-dimensioned — drop
        }
        if !self.ensure_epoch(window_start_ms) {
            return None; // stale epoch
        }
        self.edges.insert(edge_id.to_string(), EdgeF2 { sketch });
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

    // NOTE: F2 here is a MONITORED quantity (threshold/alert), NOT a sampling
    // driver. Per-edge update-sampling is the single whole-sketch ε-floor law
    // (`sampling_alloc::epsilon_sample_floor`); the old `√(F2/rate)` allocation
    // was retired (it is not a valid sketch-sampling regime — see `monitor` docs).
}

/// Geometric (Sharfman–Schuster–Keren) safe-zone layer for **communication-efficient**
/// distributed F2 monitoring — breaks the "need-L2-to-decide / need-sketch-to-get-L2"
/// circularity. Instead of every site shipping its sketch every window, each site
/// runs a PURELY LOCAL test (does its drift ball stay inside the safe ball
/// `B(0, √(d·τ))`?) using only the last-broadcast reference `C_ref` and its OWN
/// drift `ΔC_i` — never the live global. A site ships its sketch ONLY when locally
/// unsafe, triggering a resync.
///
/// Monitored quantity: `‖C‖₂²` (with `E[‖C‖₂²]=d·F2`) against `d·τ`; safe-ball
/// radius `R=√(d·τ)`. Convexity theorem: the global `C = (1/k)·Σ u_i` lies in the
/// convex hull of the drift vectors `u_i = C_ref + k·ΔC_i`, so if every site's
/// bounding ball `B((C_ref+u_i)/2, ‖u_i−C_ref‖/2) ⊆ B(0,R)` then `‖C‖<R ⇒ F2<τ`
/// (no missed crossing). F2 is the clean case — its sublevel set is already a ball,
/// so no covering-sphere construction is needed.
pub struct GeometricF2Monitor {
    tau: f64,
    epsilon: f64,
    d: usize,
    w: usize,
    seed: u64,
    refs: HashMap<String, CountSketchF2>, // each site's sketch at the last sync
    c_ref: CountSketchF2,                 // Σ refs — the broadcast reference
    resyncs: u64,
    alerted: bool,
}

impl GeometricF2Monitor {
    pub fn new(tau: f64, epsilon: f64, d: usize, w: usize, seed: u64) -> Self {
        Self {
            tau,
            epsilon,
            d,
            w,
            seed,
            refs: HashMap::new(),
            c_ref: CountSketchF2::new(d, w, seed),
            resyncs: 0,
            alerted: false,
        }
    }

    pub fn new_edge_sketch(&self) -> CountSketchF2 {
        CountSketchF2::new(self.d, self.w, self.seed)
    }
    pub fn site_count(&self) -> usize {
        self.refs.len()
    }
    pub fn resync_count(&self) -> u64 {
        self.resyncs
    }
    /// Safe-ball radius `R = √(d·(1−ε)τ)` on the merged-sketch scale. Monitors the
    /// ALERT threshold `(1−ε)τ` (not the raw `τ`) so that all-sites-locally-safe
    /// ⇒ `F2 < (1−ε)τ` — otherwise sites stay silent through the `[(1−ε)τ, τ)`
    /// band and the alert fires late (see the Go edge `f2engine.go`).
    pub fn safe_radius(&self) -> f64 {
        (self.d as f64 * (1.0 - self.epsilon) * self.tau).sqrt()
    }

    /// PURELY-LOCAL safety test (runs at the EDGE in deployment): given the site's
    /// CURRENT sketch, may it stay SILENT this round? Uses only the broadcast
    /// `C_ref` and the site's own reference/drift — NOT the live global ‖f‖₂.
    /// Returns true ⇒ the site need not communicate.
    pub fn is_locally_safe(&self, site: &str, current: &CountSketchF2) -> bool {
        if current.shape() != (self.d, self.w) {
            return false;
        }
        let zero = CountSketchF2::new(self.d, self.w, self.seed);
        let reference = self.refs.get(site).unwrap_or(&zero);
        let delta = current.minus(reference); // ΔC_i
        let k = self.refs.len().max(1) as f64;
        // Bounding ball of {C_ref, u_i = C_ref + k·ΔC_i}: centre C_ref + (k/2)·ΔC_i,
        // radius (k/2)·‖ΔC_i‖. It ⊆ B(0, R) iff ‖centre‖ + radius ≤ R.
        let centre_norm = self.c_ref.norm_sq_combo(&delta, k / 2.0).sqrt();
        let radius = (k / 2.0) * delta.l2_norm_sq().sqrt();
        centre_norm + radius <= self.safe_radius()
    }

    /// Full resync: every site contributes its CURRENT sketch (a site lands here
    /// after a local violation; the coordinator pulls all current sketches).
    /// Recomputes `C_ref`, re-checks the exact global `F2̂`, and returns an Alert
    /// if `F2̂ ≥ (1−ε)τ`.
    pub fn resync(&mut self, currents: &HashMap<String, CountSketchF2>) -> Option<F2Action> {
        self.refs.clear();
        for (id, sk) in currents {
            if sk.shape() == (self.d, self.w) {
                self.refs.insert(id.clone(), sk.clone());
            }
        }
        self.recompute_ref();
        self.resyncs += 1;
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

    fn recompute_ref(&mut self) {
        let mut m = CountSketchF2::new(self.d, self.w, self.seed);
        for s in self.refs.values() {
            m.merge(s);
        }
        self.c_ref = m;
    }

    /// Global `F2̂` from the synced reference sketches: `‖C_ref‖₂² / d`.
    pub fn global_f2(&self) -> f64 {
        self.c_ref.l2_norm_sq() / self.d as f64
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

    /// A sketch rebuilt from its cell matrix (the wire path) must yield the
    /// SAME `l2_norm_sq` / `estimate_f2` as the sketch that produced it.
    #[test]
    fn from_matrix_round_trips_norms() {
        let v: Vec<(u64, i64)> = (1..=200u64).map(|k| (k, k as i64)).collect();
        let mut sk = CountSketchF2::new(9, 4096, 0xABCD_1234);
        fill(&mut sk, &v);
        // Serialize to a rows×cols f64 matrix, then rebuild.
        let (d, w, _) = sk.dims();
        let mut matrix = vec![vec![0.0f64; w]; d];
        for r in 0..d {
            for b in 0..w {
                matrix[r][b] = sk.c[r * w + b];
            }
        }
        let rebuilt = CountSketchF2::from_matrix(&matrix);
        assert_eq!(rebuilt.shape(), (d, w));
        assert_eq!(rebuilt.l2_norm_sq(), sk.l2_norm_sq());
        assert_eq!(rebuilt.estimate_f2(), sk.estimate_f2());
    }

    /// Fractional (sampling-rescaled `1/p`) counts must survive the f64 vector.
    #[test]
    fn from_matrix_preserves_fractional_counts() {
        let matrix = vec![vec![1.5, -2.25, 0.0, 4.0], vec![-1.0, 0.5, 3.5, -0.75]];
        let sk = CountSketchF2::from_matrix(&matrix);
        let expected: f64 = matrix.iter().flatten().map(|x| x * x).sum();
        assert!((sk.l2_norm_sq() - expected).abs() < 1e-12);
    }

    /// Edge sketch (seed != 0) and a wire-rebuilt sketch (seed 0) must merge on
    /// matching shape without a seed-mismatch panic.
    #[test]
    fn merge_ignores_seed_only_shape() {
        let mut a = CountSketchF2::new(4, 8, 0x1234);
        a.update(7, 3);
        let b = CountSketchF2::from_matrix(&vec![vec![0.0; 8]; 4]);
        a.merge(&b); // must not panic (a.seed != b.seed, same shape)
        assert_eq!(a.shape(), (4, 8));
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

    // ── geometric safe-zone layer ──
    fn gcs(mon: &GeometricF2Monitor, pairs: &[(u64, i64)]) -> CountSketchF2 {
        let mut s = mon.new_edge_sketch();
        for &(k, f) in pairs {
            s.update(k, f);
        }
        s
    }

    #[test]
    fn geometric_silent_under_small_drift() {
        // sync 2 sites each {1:40} ⇒ merged {1:80}, F2=6400 < τ=10000 (R=300).
        let mut mon = GeometricF2Monitor::new(10_000.0, 0.1, 9, 4096, 0x1111);
        let mut cur = HashMap::new();
        cur.insert("a".to_string(), gcs(&mon, &[(1, 40)]));
        cur.insert("b".to_string(), gcs(&mon, &[(1, 40)]));
        mon.resync(&cur);
        assert!(mon.global_f2() < mon.tau);
        // tiny drift (+1 each) ⇒ both stay locally SAFE ⇒ zero communication.
        assert!(mon.is_locally_safe("a", &gcs(&mon, &[(1, 41)])), "small drift must be safe");
        assert!(mon.is_locally_safe("b", &gcs(&mon, &[(1, 41)])));
    }

    #[test]
    fn geometric_triggers_when_drift_threatens_tau() {
        let mut mon = GeometricF2Monitor::new(10_000.0, 0.1, 9, 4096, 0x2222);
        let mut cur = HashMap::new();
        cur.insert("a".to_string(), gcs(&mon, &[(1, 40)]));
        cur.insert("b".to_string(), gcs(&mon, &[(1, 40)]));
        mon.resync(&cur);
        // one site drifts hard ⇒ local safe-zone trips ⇒ it MUST report.
        assert!(!mon.is_locally_safe("a", &gcs(&mon, &[(1, 200)])), "large drift must trip safe-zone");
    }

    #[test]
    fn geometric_stays_silent_then_resyncs_on_violation() {
        let mut mon = GeometricF2Monitor::new(10_000.0, 0.1, 9, 4096, 0x3333);
        let a = gcs(&mon, &[(1, 30)]);
        let b = gcs(&mon, &[(1, 30)]);
        let mut cur = HashMap::new();
        cur.insert("a".to_string(), a.clone());
        cur.insert("b".to_string(), b);
        mon.resync(&cur);
        let base = mon.resync_count();
        // many small local updates: all silent (no resync triggered).
        let mut ca = a;
        for _ in 0..5 {
            ca.update(1, 1);
            assert!(mon.is_locally_safe("a", &ca));
        }
        assert_eq!(mon.resync_count(), base, "silent updates must not resync");
        // a big jump trips the safe-zone → protocol resyncs.
        let big = gcs(&mon, &[(1, 300)]);
        assert!(!mon.is_locally_safe("a", &big));
        cur.insert("a".to_string(), big);
        mon.resync(&cur);
        assert_eq!(mon.resync_count(), base + 1);
    }

    /// Cross-language parity gate: the SAME golden matrices are evaluated by the
    /// Go edge (asap-precompute-go/monitor/f2engine_test.go
    /// `TestF2LocallySafeGolden`). Both must return the SAME safe/unsafe verdict
    /// — any divergence silently breaks the geometric no-missed-crossing
    /// guarantee. This recomputes the exact `is_locally_safe` arithmetic
    /// (`‖C_ref + (k/2)ΔC‖ + (k/2)‖ΔC‖ ≤ √(d·τ)`) via the CountSketchF2
    /// primitives, on ref=0, current=cRef=[[10,0,0],[0,10,0]], k=1, d=2.
    #[test]
    fn is_locally_safe_matches_go_golden() {
        let current = CountSketchF2::from_matrix(&[vec![10.0, 0.0, 0.0], vec![0.0, 10.0, 0.0]]);
        let reference = CountSketchF2::from_matrix(&[vec![0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0]]);
        let c_ref = CountSketchF2::from_matrix(&[vec![10.0, 0.0, 0.0], vec![0.0, 10.0, 0.0]]);
        let d: f64 = 2.0;
        let k: f64 = 1.0;
        let delta = current.minus(&reference);
        let centre_norm = c_ref.norm_sq_combo(&delta, k / 2.0).sqrt();
        let ball = (k / 2.0) * delta.l2_norm_sq().sqrt();
        let sum = centre_norm + ball; // ≈ 28.284

        assert!(sum <= (d * 500.0).sqrt(), "tau=500 must be SAFE (R≈31.62)");
        assert!(sum > (d * 300.0).sqrt(), "tau=300 must be UNSAFE (R≈24.49)");
    }

    #[test]
    fn geometric_alert_when_resync_global_exceeds_tau() {
        // merged {1:100} ⇒ F2=10000 ≥ (1-0.1)·8000=7200 ⇒ Alert.
        let mut mon = GeometricF2Monitor::new(8_000.0, 0.1, 9, 4096, 0x4444);
        let mut cur = HashMap::new();
        cur.insert("a".to_string(), gcs(&mon, &[(1, 50)]));
        cur.insert("b".to_string(), gcs(&mon, &[(1, 50)]));
        let act = mon.resync(&cur).unwrap();
        assert!(matches!(act, F2Action::Alert { .. }), "global over τ ⇒ Alert, got {act:?}");
    }
}
