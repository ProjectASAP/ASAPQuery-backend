//! GOS isotropic threshold closed forms — the per-family formulas from the
//! unified error/threshold/cost framework (see
//! `ASAPCollector/docs/design-gos-unified-edge-telemetry.md` §8 /
//! `docs/sampling-cdm-gos-derivations.md` §8).
//!
//! These are the UNIFORM (isotropic) case: every cell/bucket in scope shares
//! one threshold `T`, derived in closed form from the accuracy budget with no
//! iterative solve needed. This is what the shipped ASAPCollector edge side
//! (the 2026-07 insert-time-GOS redesign) actually runs today.
//!
//! ANISOTROPIC (gradient-weighted, non-uniform `T_j` per cell) per-cell
//! water-filling is intentionally NOT included here. It is only worked out
//! for CountSketch (design §7), and — per the design doc's own "Open items"
//! (§11) — its `Activity_j` input assumes a single, uniformly-timed snapshot
//! reference, an assumption the shipped async-per-cell-reset insert-time
//! model breaks. Whether the closed-form water-filling solution still holds
//! under a redefined (e.g. per-cell EMA) activity input has not been checked.
//! Add it back once that is resolved — don't wire a stale solver in the
//! meantime.

/// F2 isotropic closed form: `T = ε·‖Ĉ‖ / (2·k·√(d·w))` (design §7/§8.3).
///
/// The relative-error threshold for CountSketch's `F2 = ‖f‖₂²` / point-query
/// readout when all cells are treated uniformly — it scales with the current
/// norm `‖Ĉ‖`, so relative error stays bounded as the sketch grows. Returns
/// `+∞` for degenerate dims (no gating).
pub fn f2_isotropic_threshold(epsilon: f64, norm: f64, k: u32, d: usize, w: usize) -> f64 {
    let k = k.max(1) as f64;
    let n = (d * w) as f64;
    if n <= 0.0 {
        return f64::INFINITY;
    }
    epsilon * norm / (2.0 * k * n.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f2_closed_form_matches_formula() {
        // T = ε‖Ĉ‖/(2k√(dw)); ε=0.1, ‖Ĉ‖=1000, k=4, d=5, w=256
        let t = f2_isotropic_threshold(0.1, 1000.0, 4, 5, 256);
        let expect = 0.1 * 1000.0 / (2.0 * 4.0 * ((5 * 256) as f64).sqrt());
        assert!((t - expect).abs() < 1e-9, "got {t} expect {expect}");
    }

    #[test]
    fn degenerate_dims_return_infinity() {
        assert_eq!(f2_isotropic_threshold(0.1, 1000.0, 4, 0, 256), f64::INFINITY);
        assert_eq!(f2_isotropic_threshold(0.1, 1000.0, 4, 5, 0), f64::INFINITY);
    }
}
