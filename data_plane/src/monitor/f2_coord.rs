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

/// An action the F2 coordinator wants the transport to perform.
#[derive(Clone, Debug, PartialEq)]
pub enum F2Out {
    /// Global `F2̂` crossed `(1−ε)τ` — fire once per epoch.
    Alert {
        f2_estimate: f64,
        tau: f64,
        window_start_ms: u64,
    },
    /// Geometric mode: broadcast the refreshed reference `C_ref = Σ latest_i`
    /// (as a cell matrix) and the site count `k` to every edge, so each can run
    /// its local safe-zone test. The server serializes the matrix and counts the
    /// egress bytes.
    RefBroadcast {
        round: u64,
        window_start_ms: u64,
        k: u64,
        c_ref: Vec<Vec<f64>>,
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
            self.alerted = false;
            return true;
        }
        false
    }

    /// Merge all latest per-edge references into one sketch.
    fn merged(&self) -> CountSketchF2 {
        let mut m = CountSketchF2::new(self.d, self.w, 0);
        for sk in self.latest.values() {
            m.merge(sk);
        }
        m
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
        if matrix.len() != self.d || matrix.first().map(|r| r.len()) != Some(self.w) {
            return Vec::new(); // mis-dimensioned — drop
        }
        if !self.ensure_epoch(window_start_ms) {
            return Vec::new(); // stale epoch
        }
        self.latest
            .insert(edge_id.to_string(), CountSketchF2::from_matrix(&matrix));

        let merged = self.merged();
        let mut out = Vec::new();
        let f2 = merged.estimate_f2();
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
            out.push(F2Out::RefBroadcast {
                round: self.round,
                window_start_ms: self.window_start_ms,
                k: self.latest.len() as u64,
                c_ref: merged.to_matrix(),
            });
        }
        out
    }

    /// Current global `F2̂` (test/eval ground-truth readout).
    pub fn global_f2(&self) -> f64 {
        self.merged().estimate_f2()
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
}
