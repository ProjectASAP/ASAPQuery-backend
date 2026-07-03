//! Generic Geometric-Monitoring local constraint — the unification of the scalar
//! CMY slack countdown and the F2 ball monitor into one `f(x̄) ∈ [L,U]` safe-zone
//! test parameterized by the function's gradient and Hessian eigenvalue bounds
//! (AutoMon/ADCD). See `ASAPCollector/docs/design-gos-unified-edge-telemetry.md`
//! §7.
//!
//! A site is **locally safe** iff, over the bounding ball of its drift (the
//! Sharfman–Schuster–Keren construction: centre `(k/2)·δ`, radius `(k/2)‖δ‖` in
//! `Δ = x−x₀` coordinates), the DC over/under-estimates of `f` stay inside the
//! admissible band `[L,U]`:
//!
//! ```text
//!   max_ball [ f(x₀) + g·Δ + ½·λ_max·‖Δ‖² ]  ≤  U
//!   min_ball [ f(x₀) + g·Δ + ½·λ_min·‖Δ‖² ]  ≥  L
//! ```
//!
//! The extreme of a quadratic over a ball has a closed form (below). Special
//! cases: `λ=0` (linear `sum`/`cms_point`) → a **slab**; constant Hessian `2I`
//! (`F2 = ‖x‖²`) → a **ball** identical to the hand-derived `f2::is_locally_safe`
//! (verified by test). Non-constant Hessians take `(λ_min,λ_max)` from AutoMon's
//! numerical eigenvalue optimization (a later phase).

/// L2 norm of a slice.
fn norm(v: &[f64]) -> f64 {
    v.iter().map(|x| x * x).sum::<f64>().sqrt()
}

/// The max (`want_max`) or min of `f_x0 + g·Δ + ½·λ·‖Δ‖²` over the ball
/// `‖Δ − c‖ ≤ ρ`. Handles all signs of `λ` via the completed square
/// `q = f_x0 + ½λ‖Δ + g/λ‖² − ‖g‖²/(2λ)`.
fn quad_extreme_over_ball(
    f_x0: f64,
    g: &[f64],
    lambda: f64,
    c: &[f64],
    rho: f64,
    want_max: bool,
) -> f64 {
    debug_assert_eq!(g.len(), c.len());
    if lambda == 0.0 {
        // Linear: extreme at c ± ρ·ĝ.
        let gc: f64 = g.iter().zip(c).map(|(gi, ci)| gi * ci).sum();
        let gn = norm(g);
        return if want_max { f_x0 + gc + gn * rho } else { f_x0 + gc - gn * rho };
    }
    // a = g/λ; shift = ‖c + a‖. Extremes of ‖Δ + a‖² over the ball:
    //   max = (shift + ρ)²,  min = max(0, shift − ρ)².
    let shift = {
        let mut s = 0.0;
        for i in 0..g.len() {
            let v = c[i] + g[i] / lambda;
            s += v * v;
        }
        s.sqrt()
    };
    let max_n2 = (shift + rho).powi(2);
    let min_n2 = (shift - rho).max(0.0).powi(2);
    let gn2: f64 = g.iter().map(|x| x * x).sum();
    let base = f_x0 - gn2 / (2.0 * lambda);
    let half = 0.5 * lambda;
    // To MAXimize q use the larger ‖·‖² when half>0, the smaller when half<0.
    let n2 = if (want_max && half > 0.0) || (!want_max && half < 0.0) {
        max_n2
    } else {
        min_n2
    };
    base + half * n2
}

/// A function's local geometric-monitoring constraint at a reference point `x₀`.
/// `grad = ∇f(x₀)`; `lambda_min ≤ 0 ≤ lambda_max` bound the Hessian spectrum over
/// the monitored neighborhood; `[l, u]` is the admissible band on `f`.
#[derive(Clone, Debug)]
pub struct LocalConstraint {
    pub grad: Vec<f64>,
    pub lambda_min: f64,
    pub lambda_max: f64,
    pub l: f64,
    pub u: f64,
    pub f_x0: f64,
}

impl LocalConstraint {
    /// The F2 (`f = ‖x‖₂²`) constraint at reference `c_ref` for the one-sided
    /// alert threshold `‖C‖² ≤ u` (typically `u = d·(1−ε)·τ`). Hessian is `2I`,
    /// so `λ_min = λ_max = 2`, `∇f = 2·c_ref`, `f(x₀) = ‖c_ref‖²`.
    pub fn f2(c_ref: &[f64], u: f64) -> Self {
        let f_x0 = c_ref.iter().map(|x| x * x).sum();
        Self {
            grad: c_ref.iter().map(|x| 2.0 * x).collect(),
            lambda_min: 2.0,
            lambda_max: 2.0,
            l: f64::NEG_INFINITY,
            u,
            f_x0,
        }
    }

    /// A linear functional `f(x) = ⟨a, x⟩` monitored in `[l, u]` (Hessian 0 →
    /// slab safe zone). Subsumes `sum`, `cms_point`, and `linear_buckets`.
    pub fn linear(a: &[f64], f_x0: f64, l: f64, u: f64) -> Self {
        Self {
            grad: a.to_vec(),
            lambda_min: 0.0,
            lambda_max: 0.0,
            l,
            u,
            f_x0,
        }
    }
}

/// Is the site locally safe? `drift = x − x₀` (this site's change since its last
/// sync), `k` sites. True ⇒ the site may stay silent this round; the DC bounds
/// over its drift ball are guaranteed inside `[L, U]`.
pub fn is_locally_safe(c: &LocalConstraint, drift: &[f64], k: usize) -> bool {
    if drift.len() != c.grad.len() {
        return false;
    }
    // Pure-linear fast path (λ=0): the tight test is the SLAB — only the drift's
    // projection onto ∇f moves `f`, so off-axis drift is ignored (the isotropic
    // ball would be needlessly conservative). Even-split per-site bound:
    //   f(x₀) + k·⟨g,δ⟩ ∈ [L,U]  ⇒  Σ_i ⟨g,δ_i⟩ ∈ [L−f₀,U−f₀]  ⇒ global safe.
    if c.lambda_min == 0.0 && c.lambda_max == 0.0 {
        let proj: f64 = c.grad.iter().zip(drift).map(|(g, d)| g * d).sum();
        let val = c.f_x0 + k.max(1) as f64 * proj;
        return val <= c.u && val >= c.l;
    }
    let half_k = k.max(1) as f64 / 2.0;
    let centre: Vec<f64> = drift.iter().map(|d| half_k * d).collect();
    let rho = half_k * norm(drift);
    let upper = quad_extreme_over_ball(c.f_x0, &c.grad, c.lambda_max, &centre, rho, true);
    let lower = quad_extreme_over_ball(c.f_x0, &c.grad, c.lambda_min, &centre, rho, false);
    upper <= c.u && lower >= c.l
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm2(v: &[f64]) -> f64 {
        v.iter().map(|x| x * x).sum()
    }

    /// The generic F2 constraint must give the SAME verdict as the hand-derived
    /// ball test `‖c_ref + (k/2)δ‖ + (k/2)‖δ‖ ≤ √u`.
    #[test]
    fn f2_constraint_matches_ball_test() {
        let c_ref = vec![10.0, 0.0, -4.0, 3.0];
        let drift = vec![1.0, 2.0, 0.0, -1.0];
        let k = 3usize;
        for &u in &[50.0, 200.0, 600.0, 2000.0] {
            let c = LocalConstraint::f2(&c_ref, u);
            let generic = is_locally_safe(&c, &drift, k);
            // direct ball test
            let hk = k as f64 / 2.0;
            let combo: f64 = c_ref
                .iter()
                .zip(&drift)
                .map(|(r, d)| {
                    let v = r + hk * d;
                    v * v
                })
                .sum();
            let ball = combo.sqrt() + hk * norm2(&drift).sqrt();
            let direct = ball <= u.sqrt();
            assert_eq!(generic, direct, "u={u}: generic={generic} direct={direct}");
        }
    }

    /// Linear functional → slab: safe iff ⟨a,·⟩ over the drift ball stays in [L,U].
    #[test]
    fn linear_is_a_slab() {
        // f(x)=x[0] (a=e0), x0 gives f_x0=100, band [90,110], 1 site.
        let a = vec![1.0, 0.0, 0.0];
        let c = LocalConstraint::linear(&a, 100.0, 90.0, 110.0);
        // small drift on the monitored coord stays safe; large trips.
        assert!(is_locally_safe(&c, &[5.0, 3.0, 9.0], 1)); // f moves +5, within [90,110]
        assert!(!is_locally_safe(&c, &[15.0, 0.0, 0.0], 1)); // f→115 > 110
        assert!(!is_locally_safe(&c, &[-20.0, 0.0, 0.0], 1)); // f→80 < 90
        // off-monitored-axis drift never moves f (a·δ = 0) → always safe.
        assert!(is_locally_safe(&c, &[0.0, 1000.0, -500.0], 1));
    }

    #[test]
    fn f2_small_drift_safe_large_unsafe() {
        let c_ref = vec![40.0, 0.0, 0.0];
        // u = d(1-ε)τ scale; pick u so the reference is comfortably inside.
        let c = LocalConstraint::f2(&c_ref, 10_000.0);
        assert!(is_locally_safe(&c, &[1.0, 0.0, 0.0], 2), "tiny drift safe");
        assert!(!is_locally_safe(&c, &[500.0, 0.0, 0.0], 2), "huge drift unsafe");
    }
}
