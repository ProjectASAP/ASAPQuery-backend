//! GOS per-cell threshold allocation — the water-filling core of the unified
//! error/threshold/cost framework (see
//! `ASAPCollector/docs/design-gos-unified-edge-telemetry.md`).
//!
//! Given, per Count-Sketch cell `j`, its activity `V_j` (per-window change mass)
//! and its sensitivity `|g_j| = |∂f/∂x_j|` to the monitored/queried function,
//! allocate a per-cell transmission threshold `T_j` that minimizes communication
//! `Σ_j V_j/T_j` subject to the (relative) accuracy budget
//!
//! ```text
//!   k·Σ_j |g_j|·T_j  ≤  B            (B = ε·‖Ĉ‖² for F2, ε·‖f‖ for a query)
//! ```
//!
//! plus box clamps: the OctoSketch query cap `T_q`, the freshness cap `V_j·Δ*`,
//! and the sampling-coupling floor `√(V_j(1−p)/p)` ("don't transmit finer than
//! you sample"). The KKT solution is the classic water-filling
//! `T_j ∝ √(V_j/|g_j|)`; box constraints are handled by iterative
//! redistribution.
//!
//! NOTE (design §5): the memory/compute/communication cost *weights* do NOT
//! enter here — on the threshold axis all three costs are `∝ Σ V_j/T_j`, so the
//! accuracy budget pins `T_j` regardless of the weights (the weights instead
//! select the *structural* knobs: uniform-vs-anisotropic, delta-vs-full,
//! sampling, refs). This module therefore takes only the accuracy budget and the
//! box bounds — a clean validation of that separability.

/// Per-cell inputs to the allocator.
#[derive(Debug, Clone, Copy)]
pub struct CellInput {
    /// `|g_j| = |∂f/∂x_j|` — the monitored function's sensitivity to this cell
    /// (for a pure linear query, `|a_j|`). Non-positive ⇒ the cell is irrelevant
    /// to the function and is bounded only by the caps.
    pub grad: f64,
    /// `V_j` — per-window change activity on this cell (from workload telemetry).
    pub activity: f64,
}

/// Allocation parameters (the relative budget and the box bounds).
#[derive(Debug, Clone, Copy)]
pub struct AllocParams {
    /// Accuracy budget `B` (relative: `ε·‖Ĉ‖²` for F2, `ε·‖f‖` for a query).
    pub budget: f64,
    /// Number of sites `k` (the merged error is `≤ k·T_j` per cell).
    pub k: u32,
    /// OctoSketch query cap `T_q` (bounds every cell for whole-sketch
    /// queryability). Use `f64::INFINITY` for no cap.
    pub t_query_cap: f64,
    /// Sampling rate `p` for the coupling floor `√(V_j(1−p)/p)`. `p ≥ 1` ⇒ no
    /// sampling ⇒ no floor.
    pub sample_p: f64,
    /// Freshness bound `Δ*` (max staleness age); per-cell cap is `V_j·Δ*`. Use
    /// `f64::INFINITY` for no freshness constraint. Same time unit as `activity`.
    pub fresh_delta: f64,
}

impl AllocParams {
    /// Sampling-coupling floor for a cell of activity `V`: the delta must exceed
    /// the sampling noise scale `√(V(1−p)/p)`, else it is transmitting noise.
    fn floor(&self, activity: f64) -> f64 {
        if self.sample_p >= 1.0 || self.sample_p <= 0.0 || activity <= 0.0 {
            return 0.0;
        }
        (activity * (1.0 - self.sample_p) / self.sample_p).sqrt()
    }

    /// Upper cap for a cell of activity `V`: min(query cap, freshness cap).
    fn cap(&self, activity: f64) -> f64 {
        let fresh = if self.fresh_delta.is_finite() {
            activity * self.fresh_delta
        } else {
            f64::INFINITY
        };
        self.t_query_cap.min(fresh)
    }
}

/// F2 isotropic closed form: `T = ε·‖Ĉ‖ / (2·k·√(d·w))` (design §7).
///
/// The relative-error threshold for `F2 = ‖f‖₂²` when all cells are treated
/// uniformly — it scales with the current norm `‖Ĉ‖`, so relative error stays
/// bounded as the sketch grows. Returns `+∞` for degenerate dims (no gating).
pub fn f2_isotropic_threshold(epsilon: f64, norm: f64, k: u32, d: usize, w: usize) -> f64 {
    let k = k.max(1) as f64;
    let n = (d * w) as f64;
    if n <= 0.0 {
        return f64::INFINITY;
    }
    epsilon * norm / (2.0 * k * n.sqrt())
}

/// Allocate per-cell thresholds by box-constrained water-filling.
///
/// Returns `T_j` for each input cell. Cells with non-positive `grad` or
/// `activity` are set to their cap (they consume no function budget). If the
/// floors alone exceed the budget the target is infeasible under the current
/// sampling — every cell is clamped to its floor (best effort; the caller can
/// detect infeasibility by comparing `k·Σ|g_j|T_j` to `budget`).
pub fn allocate_thresholds(cells: &[CellInput], p: &AllocParams) -> Vec<f64> {
    let k = p.k.max(1) as f64;
    let mut out = vec![0.0f64; cells.len()];

    // Partition: "active" cells participate in water-filling; irrelevant cells
    // (grad ≤ 0 or activity ≤ 0) are pinned to their cap.
    let mut free: Vec<usize> = Vec::with_capacity(cells.len());
    for (j, c) in cells.iter().enumerate() {
        if c.grad > 0.0 && c.activity > 0.0 {
            free.push(j);
        } else {
            out[j] = p.cap(c.activity).min(f64::MAX);
        }
    }

    // Per-cell price c_j = k·|g_j|; budget consumed by an active cell = c_j·T_j.
    let mut budget_left = p.budget;
    // Iteratively water-fill, pulling cells that hit a box bound out of `free`.
    loop {
        // denom = Σ_free √(c_j·V_j),  c_j = k·grad_j
        let denom: f64 = free
            .iter()
            .map(|&j| (k * cells[j].grad * cells[j].activity).sqrt())
            .sum();
        if denom <= 0.0 || budget_left <= 0.0 {
            // No budget / no free cells: pin the rest to their floor (tightest).
            for &j in &free {
                out[j] = p.floor(cells[j].activity);
            }
            break;
        }
        // Scale s so Σ_free c_j·(s√(V_j/c_j)) = s·denom = budget_left.
        let s = budget_left / denom;
        let mut clamped_any = false;
        let mut next_free = Vec::with_capacity(free.len());
        for &j in &free {
            let cj = k * cells[j].grad;
            let t = s * (cells[j].activity / cj).sqrt();
            let cap = p.cap(cells[j].activity);
            let floor = p.floor(cells[j].activity);
            if t > cap {
                out[j] = cap;
                budget_left -= cj * cap;
                clamped_any = true;
            } else if t < floor {
                out[j] = floor;
                budget_left -= cj * floor;
                clamped_any = true;
            } else {
                next_free.push(j);
            }
        }
        if !clamped_any {
            // All remaining free cells fit within their box → assign & done.
            for &j in &next_free {
                let cj = k * cells[j].grad;
                out[j] = s * (cells[j].activity / cj).sqrt();
            }
            break;
        }
        free = next_free;
        if free.is_empty() {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(budget: f64, k: u32) -> AllocParams {
        AllocParams {
            budget,
            k,
            t_query_cap: f64::INFINITY,
            sample_p: 1.0,
            fresh_delta: f64::INFINITY,
        }
    }

    #[test]
    fn f2_closed_form_matches_formula() {
        // T = ε‖Ĉ‖/(2k√(dw)); ε=0.1, ‖Ĉ‖=1000, k=4, d=5, w=256
        let t = f2_isotropic_threshold(0.1, 1000.0, 4, 5, 256);
        let expect = 0.1 * 1000.0 / (2.0 * 4.0 * ((5 * 256) as f64).sqrt());
        assert!((t - expect).abs() < 1e-9, "got {t} expect {expect}");
    }

    #[test]
    fn uniform_cells_give_uniform_thresholds_and_bind_budget() {
        // Equal grad + activity ⇒ equal T; and k·Σ|g|T = budget (binding).
        let cells = vec![CellInput { grad: 2.0, activity: 100.0 }; 8];
        let p = params(500.0, 3);
        let t = allocate_thresholds(&cells, &p);
        for w in t.windows(2) {
            assert!((w[0] - w[1]).abs() < 1e-9, "thresholds must be uniform");
        }
        let used: f64 = 3.0 * cells.iter().zip(&t).map(|(c, tj)| c.grad * tj).sum::<f64>();
        assert!((used - 500.0).abs() < 1e-6, "budget must bind, used {used}");
    }

    #[test]
    fn anisotropic_water_filling_shape() {
        // T_j ∝ √(V_j/|g_j|): higher activity ⇒ larger T; higher grad ⇒ smaller T.
        let cells = vec![
            CellInput { grad: 1.0, activity: 100.0 }, // baseline
            CellInput { grad: 1.0, activity: 400.0 }, // 4× activity ⇒ 2× T
            CellInput { grad: 4.0, activity: 100.0 }, // 4× grad     ⇒ 0.5× T
        ];
        let t = allocate_thresholds(&cells, &params(1000.0, 1));
        assert!((t[1] / t[0] - 2.0).abs() < 1e-6, "activity: {} vs {}", t[1], t[0]);
        assert!((t[2] / t[0] - 0.5).abs() < 1e-6, "grad: {} vs {}", t[2], t[0]);
    }

    #[test]
    fn query_cap_clamps_and_redistributes() {
        // A generous budget would push all T large; the query cap clamps them,
        // and freed budget flows to uncapped cells (here all capped).
        let cells = vec![CellInput { grad: 1.0, activity: 100.0 }; 4];
        let mut p = params(1e9, 1);
        p.t_query_cap = 5.0;
        let t = allocate_thresholds(&cells, &p);
        for tj in &t {
            assert!(*tj <= 5.0 + 1e-9, "capped at 5, got {tj}");
        }
    }

    #[test]
    fn sampling_floor_lifts_small_thresholds() {
        // A tiny budget wants T→0, but the sampling floor √(V(1−p)/p) lifts it.
        let cells = vec![CellInput { grad: 1.0, activity: 100.0 }; 4];
        let mut p = params(1e-6, 1);
        p.sample_p = 0.5; // floor = √(100·0.5/0.5) = 10
        let t = allocate_thresholds(&cells, &p);
        for tj in &t {
            assert!((*tj - 10.0).abs() < 1e-9, "floor 10, got {tj}");
        }
    }

    #[test]
    fn irrelevant_cells_pinned_to_cap_not_budget() {
        // grad=0 cell doesn't consume the function budget; it takes the cap.
        let cells = vec![
            CellInput { grad: 0.0, activity: 100.0 }, // irrelevant
            CellInput { grad: 2.0, activity: 100.0 },
        ];
        let mut p = params(8.0, 1);
        p.t_query_cap = 7.0;
        let t = allocate_thresholds(&cells, &p);
        assert!((t[0] - 7.0).abs() < 1e-9, "irrelevant → cap, got {}", t[0]);
        // the active cell gets the whole budget: 1·2·T = 8 → T = 4 (below the cap).
        assert!((t[1] - 4.0).abs() < 1e-6, "active got {}", t[1]);
    }
}
