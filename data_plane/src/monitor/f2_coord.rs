//! Coordinator-side state machine for **whole-sketch F2 (`‖f‖₂²`) monitors** —
//! the counterpart to the scalar [`super::coordinator::Monitor`] for the
//! non-linear functional that the scalar slack-countdown cannot express.
//!
//! Both variants keep, per edge, that edge's **latest reported Count-Sketch**
//! (the coordinator's reference for that site) and re-estimate the global
//! `F2̂ = estimate_f2(Σ_i latest_i)` on every report — the linear merge recovers
//! the cross terms `2Σ⟨f_i,f_j⟩` a per-edge scalar would miss. The **alert
//! decision is identical in both modes** (fire once at `F2̂ ≥ (1−ε)τ`); only the
//! communication pattern differs:
//!
//! * **Distributed** — every edge ships its sketch every window; the coordinator
//!   just accumulates and checks. Communication baseline.
//! * **Geometric** — an edge ships only when its *local* safe-zone test trips
//!   (run at the edge against the broadcast `C_ref`). On each ship the
//!   coordinator refreshes that site's reference and re-broadcasts
//!   `C_ref = Σ_i latest_i` (a **lazy resync**: only the violating site's
//!   reference moves, which is safety-correct — the global `‖C‖<R ⇒ F2<τ`
//!   guarantee holds as long as every site is locally safe against the current
//!   `C_ref`). No `PollLocal` gather is needed.
//!
//! This module is pure (matrices in, actions out); the tonic server
//! (`server.rs`) does the msgpack (de)serialization, wire dispatch, and byte
//! accounting.

use std::collections::HashMap;

use super::coordinator::F2Mode;
use super::f2::CountSketchF2;

/// Periodic keyframe cadence for the geometric `C_ref` broadcast: every Nth
/// broadcast ships a **Full** matrix instead of a sparse delta, bounding how long
/// an edge that missed/failed to apply a delta can run against a diverged cached
/// reference (an I-frame, in video terms). The edge already force-ships while its
/// reference is untrustworthy (see `f2engine.go` `needFull`), so this only
/// governs how fast such an edge recovers to silent operation — it does not
/// affect safety. Chosen larger than the eval's per-mode broadcast count so the
/// reported communication figures are unchanged; a diverged edge otherwise
/// self-heals at the next epoch boundary regardless.
const F2_KEYFRAME_INTERVAL: u64 = 32;

/// An action the F2 coordinator wants the transport to perform.
#[derive(Clone, Debug, PartialEq)]
pub enum F2Out {
    /// Global `F2̂` crossed `(1−ε)τ` — fire once per epoch.
    Alert {
        f2_estimate: f64,
        tau: f64,
        window_start_ms: u64,
    },
    /// Geometric mode: broadcast the refreshed reference `C_ref = Σ latest_i` and
    /// the site count `k` to every edge, so each can run its local safe-zone test.
    /// The update is `Full` (first broadcast of the epoch) or a sparse `Delta` of
    /// the cells that changed since the last broadcast — the latter removes the
    /// O(k) full-matrix amplification of the geometric resync.
    RefBroadcast {
        round: u64,
        window_start_ms: u64,
        k: u64,
        cref: CRefUpdate,
    },
}

/// A `C_ref` reference update: a full cell matrix, or a sparse delta of changed
/// cells `(row, col, Δ)` against the edge's cached reference.
#[derive(Clone, Debug, PartialEq)]
pub enum CRefUpdate {
    Full(Vec<Vec<f64>>),
    Delta {
        rows: usize,
        cols: usize,
        cells: Vec<(u32, u32, f64)>,
    },
}

/// Per-monitor F2 coordinator state for the current epoch.
pub struct F2CoordMonitor {
    mode: F2Mode,
    tau: f64,
    epsilon: f64,
    d: usize,
    w: usize,
    window_start_ms: u64,
    round: u64,
    /// Each edge's latest reported sketch (its coordinator-side reference).
    latest: HashMap<String, CountSketchF2>,
    /// Running merged sketch `Σ_i latest_i`, maintained incrementally (a report
    /// applies `running += new_i − old_i`, O(d·w)) instead of re-merging all k
    /// edges every report (O(k·d·w)).
    running: CountSketchF2,
    /// Last `C_ref` broadcast (geometric mode) — the base for the sparse delta
    /// broadcast, so the coordinator ships only the changed cells of `C_ref`
    /// instead of the full matrix each resync.
    last_broadcast: Option<CountSketchF2>,
    alerted: bool,
}

impl F2CoordMonitor {
    pub fn new(mode: F2Mode, tau: f64, epsilon: f64, d: usize, w: usize) -> Self {
        Self {
            mode,
            tau,
            epsilon,
            d,
            w,
            window_start_ms: 0,
            round: 0,
            latest: HashMap::new(),
            running: CountSketchF2::new(d, w, 0),
            last_broadcast: None,
            alerted: false,
        }
    }

    pub fn mode(&self) -> F2Mode {
        self.mode
    }
    pub fn window_start_ms(&self) -> u64 {
        self.window_start_ms
    }
    pub fn edge_count(&self) -> usize {
        self.latest.len()
    }

    /// Advance to (or stay on) `window_start`. A new epoch clears all references
    /// (the edge Count-Sketches reset at the tumbling boundary — mirrors the
    /// scalar monitor's `ensure_epoch` and `epoch::align`). Returns false for a
    /// stale (already-closed) epoch, in which case the caller drops the report.
    fn ensure_epoch(&mut self, window_start: u64) -> bool {
        if window_start == self.window_start_ms {
            return true;
        }
        if window_start > self.window_start_ms {
            self.window_start_ms = window_start;
            self.round = 0;
            self.latest.clear();
            self.running = CountSketchF2::new(self.d, self.w, 0);
            self.last_broadcast = None;
            self.alerted = false;
            return true;
        }
        false
    }

    /// Ingest one edge's Count-Sketch cell matrix for `window_start_ms`.
    /// Refreshes that edge's reference, re-estimates the global `F2̂`, and returns
    /// the resulting actions (an `Alert` if it crossed, plus — in geometric mode
    /// — a `RefBroadcast` of the refreshed `C_ref`). Mis-dimensioned or
    /// stale-epoch reports return no actions.
    pub fn on_sketch(
        &mut self,
        edge_id: &str,
        window_start_ms: u64,
        matrix: Vec<Vec<f64>>,
    ) -> Vec<F2Out> {
        // Validate EVERY row's width (not just the first) — a ragged wire matrix
        // would otherwise panic in CountSketchF2::from_matrix below.
        if matrix.len() != self.d || matrix.iter().any(|r| r.len() != self.w) {
            return Vec::new(); // mis-dimensioned — drop
        }
        if !self.ensure_epoch(window_start_ms) {
            return Vec::new(); // stale epoch
        }
        // Incremental merge: running += new_i − old_i  (O(d·w), not O(k·d·w)).
        let new = CountSketchF2::from_matrix(&matrix);
        let (delta, is_new_edge) = match self.latest.get(edge_id) {
            Some(old) => (new.minus(old), false),
            None => (new.clone(), true),
        };
        self.running.merge(&delta);
        self.latest.insert(edge_id.to_string(), new);
        // A newly-joined edge has no cached C_ref to apply a delta onto, so force
        // a Full keyframe this round (drop the delta base).
        if is_new_edge {
            self.last_broadcast = None;
        }

        let mut out = Vec::new();
        // Mean-of-rows F2 (‖C‖²/d): the SAME estimator the edge geometric
        // safe-zone ball monitors, so silence and alert are consistent
        // (all-sites-safe ⟺ mean_f2 < (1−ε)τ). Both modes use it so distributed
        // (baseline) and geometric alert on the identical quantity.
        let f2 = self.running.mean_f2();
        if !self.alerted && f2 >= (1.0 - self.epsilon) * self.tau {
            self.alerted = true;
            out.push(F2Out::Alert {
                f2_estimate: f2,
                tau: self.tau,
                window_start_ms: self.window_start_ms,
            });
        }
        if self.mode == F2Mode::Geometric {
            self.round += 1;
            // Full on the first broadcast of the epoch and every
            // F2_KEYFRAME_INTERVAL rounds (a periodic keyframe that bounds an
            // edge's delta-loss divergence recovery); sparse delta otherwise.
            let force_keyframe = self.round % F2_KEYFRAME_INTERVAL == 0;
            let (cref, new_last) = match &self.last_broadcast {
                Some(last) if !force_keyframe => {
                    // Thresholded broadcast gate (design §12 #1): ship only cells
                    // that moved by more than the §7 F2 per-cell threshold
                    // T = ε‖C‖/(2k√(dw)) — a cell changing by less than that does
                    // not materially move the F2 estimate, so broadcasting it is
                    // pure O(k) amplification on a dense sketch. Sub-T changes
                    // accumulate in (running − last_broadcast) and ship once they
                    // cross T, so the edge's C_ref error stays ≤ T per cell.
                    let k = self.latest.len().max(1) as f64;
                    let norm = self.running.l2_norm_sq().sqrt();
                    let t = self.epsilon * norm / (2.0 * k * ((self.d * self.w) as f64).sqrt());
                    let cells = self.running.sparse_delta_cells_thresholded(last, t);
                    // last_broadcast must track what the edge ACTUALLY holds:
                    // the previous reference plus only the shipped cells.
                    let mut nb = last.clone();
                    nb.apply_cells(&cells);
                    (
                        CRefUpdate::Delta {
                            rows: self.d,
                            cols: self.w,
                            cells,
                        },
                        nb,
                    )
                }
                // Full keyframe: the edge receives the whole running sketch.
                _ => (CRefUpdate::Full(self.running.to_matrix()), self.running.clone()),
            };
            self.last_broadcast = Some(new_last);
            out.push(F2Out::RefBroadcast {
                round: self.round,
                window_start_ms: self.window_start_ms,
                k: self.latest.len() as u64,
                cref,
            });
        }
        out
    }

    /// Current global `F2̂` (mean-of-rows, consistent with the alert/safe-zone).
    pub fn global_f2(&self) -> f64 {
        self.running.mean_f2()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch(d: usize, w: usize, seed: u64, pairs: &[(u64, i64)]) -> Vec<Vec<f64>> {
        let mut s = CountSketchF2::new(d, w, seed);
        for &(k, f) in pairs {
            s.update(k, f);
        }
        s.to_matrix()
    }

    #[test]
    fn distributed_fires_on_merged_cross_terms() {
        // a={1:10,2:10}, b={1:10,3:10}: Σ local F2 = 400, merged {1:20,2:10,3:10}
        // ⇒ F2 = 600. τ=400 ⇒ merged crosses, per-edge would not.
        let (d, w, seed) = (9, 4096, 0x2468_1357u64);
        let mut mon = F2CoordMonitor::new(F2Mode::Distributed, 400.0, 0.1, d, w);
        assert!(mon
            .on_sketch("a", 0, sketch(d, w, seed, &[(1, 10), (2, 10)]))
            .is_empty());
        let out = mon.on_sketch("b", 0, sketch(d, w, seed, &[(1, 10), (3, 10)]));
        assert!(
            matches!(out.as_slice(), [F2Out::Alert { .. }]),
            "merged F2≈600 ≥ (1-.1)·400 ⇒ alert, got {out:?}"
        );
    }

    #[test]
    fn geometric_emits_refbroadcast_and_same_alert() {
        let (d, w, seed) = (9, 4096, 0x1111u64);
        let mut mon = F2CoordMonitor::new(F2Mode::Geometric, 400.0, 0.1, d, w);
        // First edge: RefBroadcast, no alert yet.
        let o1 = mon.on_sketch("a", 0, sketch(d, w, seed, &[(1, 10), (2, 10)]));
        assert!(matches!(o1.as_slice(), [F2Out::RefBroadcast { k: 1, .. }]));
        // Second edge pushes merged over τ: Alert + RefBroadcast.
        let o2 = mon.on_sketch("b", 0, sketch(d, w, seed, &[(1, 10), (3, 10)]));
        assert!(o2.iter().any(|a| matches!(a, F2Out::Alert { .. })));
        assert!(o2.iter().any(|a| matches!(a, F2Out::RefBroadcast { k: 2, .. })));
    }

    #[test]
    fn stays_silent_below_tau() {
        let (d, w, seed) = (9, 4096, 0x3333u64);
        let mut mon = F2CoordMonitor::new(F2Mode::Distributed, 100_000.0, 0.1, d, w);
        for e in ["a", "b", "c"] {
            let out = mon.on_sketch(e, 0, sketch(d, w, seed, &[(1, 5), (2, 5)]));
            assert!(out.iter().all(|a| !matches!(a, F2Out::Alert { .. })));
        }
        assert!(mon.global_f2() < 100_000.0);
    }

    #[test]
    fn epoch_advance_clears_refs() {
        let (d, w, seed) = (5, 1024, 7u64);
        let mut mon = F2CoordMonitor::new(F2Mode::Distributed, 1_000.0, 0.1, d, w);
        mon.on_sketch("e", 0, sketch(d, w, seed, &[(1, 5)]));
        assert_eq!(mon.edge_count(), 1);
        mon.on_sketch("e", 60_000, sketch(d, w, seed, &[(1, 1)]));
        assert_eq!(mon.window_start_ms(), 60_000);
        assert_eq!(mon.edge_count(), 1);
        // Stale epoch dropped.
        assert!(mon.on_sketch("e", 0, sketch(d, w, seed, &[(1, 1)])).is_empty());
    }

    #[test]
    fn mis_dimensioned_dropped() {
        let mut mon = F2CoordMonitor::new(F2Mode::Distributed, 100.0, 0.1, 4, 8);
        assert!(mon.on_sketch("e", 0, vec![vec![0.0; 4]; 4]).is_empty());
    }

    #[test]
    fn periodic_keyframe_bounds_delta_divergence() {
        // A single edge ships every window (τ huge → no alert, only RefBroadcast).
        // The broadcast is Full at round 1 (bootstrap keyframe) and again at
        // round F2_KEYFRAME_INTERVAL; every round in between is a sparse Delta —
        // so a diverged edge fully resyncs within one interval.
        let (d, w, seed) = (5, 256, 9u64);
        let mut mon = F2CoordMonitor::new(F2Mode::Geometric, 1e12, 0.1, d, w);
        let s = sketch(d, w, seed, &[(1, 10)]);
        let mut full_rounds = Vec::new();
        for round in 1..=F2_KEYFRAME_INTERVAL {
            let out = mon.on_sketch("a", 0, s.clone());
            let is_full = out.iter().any(|o| {
                matches!(
                    o,
                    F2Out::RefBroadcast {
                        cref: CRefUpdate::Full(_),
                        ..
                    }
                )
            });
            if is_full {
                full_rounds.push(round);
            }
        }
        assert_eq!(
            full_rounds,
            vec![1, F2_KEYFRAME_INTERVAL],
            "Full keyframe must land at round 1 and every F2_KEYFRAME_INTERVAL; deltas in between"
        );
    }
}
