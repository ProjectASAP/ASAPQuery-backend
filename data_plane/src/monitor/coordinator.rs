//! The global-threshold coordinator state machine — the Cormode–Muthukrishnan–Yi
//! distributed functional monitoring countdown, realized in the lost-mass-free
//! single-phase form.
//!
//! Per monitor `(agg_id, key)` the coordinator tracks, for each edge, the LAST
//! VALUE that edge reported (its "known value"). Because the monitored
//! functional is additive and monotone within a tumbling window, an edge that
//! has not reported is guaranteed to be within its granted slack of its known
//! value. So:
//!
//! ```text
//!   global_estimate = Σ known_value_i + departed_mass
//!   true_global     ≤ global_estimate + Σ slack_i
//! ```
//!
//! Allocating each edge `slack = Δ / (2k)` where `Δ = τ − global_estimate` and
//! `k` = #edges keeps the total uncertainty `Σ slack = Δ/2`, so
//! `true_global < (τ + global_estimate)/2 < τ` as long as `global_estimate < τ`
//! — the safety guarantee (no missed crossing). Each report raises
//! `global_estimate`, which shrinks `Δ` and hence the slack; a re-grant to all
//! edges lowers the bar for the next report. When `Δ ≤ ε·τ` the coordinator
//! fires the alert; at that point `true_global ∈ [(1−ε)τ, (1−ε)τ + ετ/2]`, i.e.
//! within `ε·τ` of `τ`.
//!
//! The baseline at each edge tracks its last reported value (NOT reset on
//! grant), which is exactly this coordinator's `known_value` — so no
//! below-slack mass is lost when the slack shrinks, and no separate poll/collect
//! round is needed. This module is pure (no I/O) and fully unit-tested; the
//! tonic server (`server.rs`) translates `Action`s to wire messages.

use std::collections::HashMap;

use super::sampling_alloc::{allocate_sample_rates, epsilon_sample_floor};

/// Static configuration for one monitor, sourced from the streaming-config
/// `monitors:` section (τ authoritative here, not at the edge).
#[derive(Clone, Debug)]
pub struct MonitorConfig {
    pub agg_id: u64,
    pub key: Vec<u8>,
    pub tau: f64,
    pub epsilon: f64,
    pub window_ms: u64,
}

/// An action the coordinator wants the transport to perform.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Send a per-round slack grant to one edge. `sample_p` is the distributed-
    /// NitroSketch update-sampling probability the coordinator allocates for this
    /// edge (1.0 = no sampling); it is an ADDITIONAL field orthogonal to the
    /// slack countdown.
    Grant {
        edge_id: String,
        round: u64,
        local_slack: f64,
        window_start_ms: u64,
        sample_p: f64,
    },
    /// The global aggregate crossed τ (within ε): fire exactly once per epoch.
    Alert {
        global_estimate: f64,
        tau: f64,
        window_start_ms: u64,
    },
}

#[derive(Clone, Debug, Default)]
struct EdgeView {
    known_value: f64,
    last_seq: u64,
    seen_seq: bool,
    /// Last `rate` (items/window) the edge reported; 0 = unknown ⇒ no sampling
    /// for this edge. Feeds the coordinated `p_i` allocation.
    rate: f64,
}

/// One monitor's coordinator state for the current epoch.
pub struct Monitor {
    cfg: MonitorConfig,
    window_start_ms: u64,
    round: u64,
    edges: HashMap<String, EdgeView>,
    departed_mass: f64,
    alerted: bool,
}

impl Monitor {
    pub fn new(cfg: MonitorConfig) -> Self {
        Self {
            cfg,
            window_start_ms: 0,
            round: 0,
            edges: HashMap::new(),
            departed_mass: 0.0,
            alerted: false,
        }
    }

    pub fn window_start_ms(&self) -> u64 {
        self.window_start_ms
    }
    pub fn round(&self) -> u64 {
        self.round
    }
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Σ known values + departed mass — a lower bound on the true global.
    pub fn global_estimate(&self) -> f64 {
        self.departed_mass + self.edges.values().map(|e| e.known_value).sum::<f64>()
    }

    /// Δ = τ − estimate (clamped at 0).
    fn gap(&self) -> f64 {
        (self.cfg.tau - self.global_estimate()).max(0.0)
    }

    /// Current per-edge slack = Δ / (2k); 0 when there are no edges.
    fn slack(&self) -> f64 {
        let k = self.edges.len();
        if k == 0 {
            return 0.0;
        }
        self.gap() / (2.0 * k as f64)
    }

    /// Ensure the monitor is on the epoch for `window_start`. Advancing resets
    /// the epoch (sketches reset at the boundary, so known values restart at 0).
    /// Returns false if the timestamp is for a stale (already-closed) epoch, in
    /// which case the caller should drop the event.
    fn ensure_epoch(&mut self, window_start: u64) -> bool {
        if window_start == self.window_start_ms {
            return true;
        }
        if window_start > self.window_start_ms {
            self.window_start_ms = window_start;
            self.round = 0;
            self.departed_mass = 0.0;
            self.alerted = false;
            for e in self.edges.values_mut() {
                *e = EdgeView::default();
            }
            return true;
        }
        false // stale epoch
    }

    /// Register (or refresh) an edge for the epoch it reports as
    /// `window_start_ms` (the edge's own aligned window start, which avoids any
    /// coordinator clock skew). Adds the edge to the membership set and re-grants
    /// the (now smaller) slack to everyone, since k changed.
    pub fn on_register(
        &mut self,
        edge_id: &str,
        epoch_window_ms: u64,
        window_start_ms: u64,
    ) -> Vec<Action> {
        // Alignment guard: a mismatched window size would silently corrupt the
        // global estimate. Refuse the registration (no grants) and let the
        // caller log/close the stream.
        if epoch_window_ms != self.cfg.window_ms {
            return Vec::new();
        }
        if !self.ensure_epoch(window_start_ms) {
            return Vec::new();
        }
        self.edges.entry(edge_id.to_string()).or_default();
        self.rebroadcast()
    }

    /// Handle an edge report. Returns grants (re-broadcast with shrunken slack)
    /// or an alert. Stale-epoch, unknown-edge, and duplicate-seq reports are
    /// dropped.
    pub fn on_report(
        &mut self,
        edge_id: &str,
        window_start_ms: u64,
        local_value: f64,
        seq: u64,
        rate: f64,
    ) -> Vec<Action> {
        if window_start_ms != self.window_start_ms {
            // Could be a future epoch we haven't advanced to yet — advance on
            // strictly-greater, drop on stale.
            if window_start_ms > self.window_start_ms {
                if !self.ensure_epoch(window_start_ms) {
                    return Vec::new();
                }
            } else {
                return Vec::new();
            }
        }
        let Some(edge) = self.edges.get_mut(edge_id) else {
            return Vec::new(); // unknown edge — registration precedes reports
        };
        if edge.seen_seq && seq <= edge.last_seq {
            return Vec::new(); // idempotent re-delivery
        }
        edge.last_seq = seq;
        edge.seen_seq = true;
        // Monotone within an epoch: never let a known value go backwards.
        if local_value > edge.known_value {
            edge.known_value = local_value;
        }
        // Track the edge's latest observed per-window rate (ignore non-positive
        // = unknown), which drives the coordinated sampling allocation.
        if rate > 0.0 {
            edge.rate = rate;
        }
        self.rebroadcast()
    }

    /// An edge left (stream closed). Fold its last-known value into departed mass
    /// so the estimate keeps it, drop it from membership, and re-grant.
    pub fn on_leave(&mut self, edge_id: &str) -> Vec<Action> {
        if let Some(e) = self.edges.remove(edge_id) {
            self.departed_mass += e.known_value;
        }
        self.rebroadcast()
    }

    /// Coordinated update-sampling allocation for the current edge set, keyed by
    /// edge id. Returns `p_i ∈ (0,1]` per edge, each clamped up to the CDM-
    /// threshold coupling floor so sampling noise stays within ε.
    ///
    /// The merged sampling-variance budget `V` is derived from what the
    /// coordinator already holds: the CDM tolerance on the monitored value is
    /// `ε·τ`, and update-sampling injects a std-dev of `√(Σ f_i(1−p_i)/p_i)` into
    /// `Σf̂`. Keeping that band within the threshold tolerance means
    /// `√V ≤ ε·τ`, i.e. **`V = (ε·τ)²`** — the sampling noise is absorbed within
    /// the CDM band (the "ε_cdm ≳ ε_s" coupling). Lacking a per-key frequency
    /// split, we use each edge's `rate` as the `freqs` proxy (so `p_i ∝
    /// 1/√rate_i`). With <2 edges or all rates unknown the allocation degenerates
    /// to `p=1` everywhere (no sampling).
    fn allocate_p(&self) -> HashMap<String, f64> {
        // Stable edge order so the rate vector aligns with the p vector.
        let ids: Vec<&String> = self.edges.keys().collect();
        let rates: Vec<f64> = ids.iter().map(|id| self.edges[*id].rate).collect();
        // freqs proxy = rates (no per-key split available at the coordinator).
        let var_budget = {
            let band = self.cfg.epsilon * self.cfg.tau;
            band * band
        };
        let mut p_vec = allocate_sample_rates(&rates, &rates, var_budget);
        // Enforce the CDM-threshold coupling floor per edge: never sample so hard
        // that ε_s = √((1−p)/(p·rate)) exceeds the agg's ε.
        for (i, &rate) in rates.iter().enumerate() {
            let floor = epsilon_sample_floor(self.cfg.epsilon, rate);
            if p_vec[i] < floor {
                p_vec[i] = floor;
            }
        }
        ids.into_iter()
            .cloned()
            .zip(p_vec)
            .map(|(id, p)| {
                // Single edge or unknown rate ⇒ no sampling (p=1).
                let p = if p > 0.0 && p <= 1.0 { p } else { 1.0 };
                (id, p)
            })
            .collect()
    }

    /// Recompute Δ; fire the alert once if within ε, otherwise advance the round
    /// and emit a fresh slack grant to every edge.
    fn rebroadcast(&mut self) -> Vec<Action> {
        if self.alerted || self.edges.is_empty() {
            return Vec::new();
        }
        let est = self.global_estimate();
        if self.gap() <= self.cfg.epsilon * self.cfg.tau {
            self.alerted = true;
            return vec![Action::Alert {
                global_estimate: est,
                tau: self.cfg.tau,
                window_start_ms: self.window_start_ms,
            }];
        }
        self.round += 1;
        let slack = self.slack();
        let round = self.round;
        let ws = self.window_start_ms;
        // With a single edge there is no rate vector to coordinate over → p=1.
        let p_by_edge = if self.edges.len() >= 2 {
            self.allocate_p()
        } else {
            HashMap::new()
        };
        self.edges
            .keys()
            .map(|edge_id| Action::Grant {
                edge_id: edge_id.clone(),
                round,
                local_slack: slack,
                window_start_ms: ws,
                sample_p: p_by_edge.get(edge_id).copied().unwrap_or(1.0),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(tau: f64) -> MonitorConfig {
        MonitorConfig {
            agg_id: 1,
            key: Vec::new(),
            tau,
            epsilon: 0.05,
            window_ms: 60_000,
        }
    }

    fn grant_for<'a>(actions: &'a [Action], edge: &str) -> Option<&'a Action> {
        actions
            .iter()
            .find(|a| matches!(a, Action::Grant { edge_id, .. } if edge_id == edge))
    }

    fn slack_in(actions: &[Action], edge: &str) -> f64 {
        match grant_for(actions, edge) {
            Some(Action::Grant { local_slack, .. }) => *local_slack,
            _ => panic!("no grant for {edge}"),
        }
    }

    fn sample_p_in(actions: &[Action], edge: &str) -> f64 {
        match grant_for(actions, edge) {
            Some(Action::Grant { sample_p, .. }) => *sample_p,
            _ => panic!("no grant for {edge}"),
        }
    }

    #[test]
    fn registration_grants_initial_slack() {
        let mut m = Monitor::new(cfg(100.0));
        let a = m.on_register("e1", 60_000, 0);
        // Δ=100, k=1 → slack=50.
        assert_eq!(slack_in(&a, "e1"), 50.0);
        assert_eq!(m.round(), 1);
    }

    #[test]
    fn alignment_guard_rejects_mismatched_window() {
        let mut m = Monitor::new(cfg(100.0));
        let a = m.on_register("e1", 30_000, 0); // wrong window size
        assert!(a.is_empty());
        assert_eq!(m.edge_count(), 0);
    }

    #[test]
    fn slack_shrinks_as_estimate_climbs() {
        let mut m = Monitor::new(cfg(100.0));
        m.on_register("e1", 60_000, 0); // slack 50
                                        // e1 reports 50 → estimate=50, Δ=50, slack=25.
        let a = m.on_report("e1", 0, 50.0, 1, 0.0);
        assert_eq!(slack_in(&a, "e1"), 25.0);
        // reports 75 → estimate=75, Δ=25, slack=12.5.
        let a = m.on_report("e1", 0, 75.0, 2, 0.0);
        assert_eq!(slack_in(&a, "e1"), 12.5);
    }

    #[test]
    fn fires_alert_within_epsilon() {
        let mut m = Monitor::new(cfg(100.0)); // epsilon 0.05 → alert when Δ ≤ 5
        m.on_register("e1", 60_000, 0);
        assert!(matches!(
            m.on_report("e1", 0, 50.0, 1, 0.0).as_slice(),
            [Action::Grant { .. }]
        ));
        assert!(matches!(
            m.on_report("e1", 0, 90.0, 2, 0.0).as_slice(),
            [Action::Grant { .. }]
        ));
        // estimate 96 → Δ=4 ≤ 5 → alert.
        let a = m.on_report("e1", 0, 96.0, 3, 0.0);
        match a.as_slice() {
            [Action::Alert {
                global_estimate,
                tau,
                ..
            }] => {
                assert_eq!(*global_estimate, 96.0);
                assert_eq!(*tau, 100.0);
            }
            other => panic!("expected alert, got {other:?}"),
        }
        // Further reports do not re-fire.
        assert!(m.on_report("e1", 0, 200.0, 4, 0.0).is_empty());
    }

    #[test]
    fn stays_silent_below_tau() {
        let mut m = Monitor::new(cfg(100.0));
        m.on_register("e1", 60_000, 0);
        m.on_register("e2", 60_000, 0);
        // Both report modestly; estimate stays well below τ → only grants, never alert.
        for seq in 1..=5 {
            let a = m.on_report("e1", 0, seq as f64 * 2.0, seq, 0.0);
            assert!(a.iter().all(|x| matches!(x, Action::Grant { .. })));
            let b = m.on_report("e2", 0, seq as f64 * 2.0, seq, 0.0);
            assert!(b.iter().all(|x| matches!(x, Action::Grant { .. })));
        }
        assert!(m.global_estimate() < 100.0);
    }

    #[test]
    fn multi_edge_grants_all_and_uncertainty_bounded() {
        let mut m = Monitor::new(cfg(120.0));
        m.on_register("e1", 60_000, 0);
        let a = m.on_register("e2", 60_000, 0);
        m.on_register("e3", 60_000, 0);
        // After 3 edges, a register re-grants ALL three; Δ=120, k=3 → slack=20.
        let a3 = m.on_report("e1", 0, 0.0, 1, 0.0);
        assert_eq!(a3.len(), 3, "re-grant should reach all edges");
        let _ = a;
        // Safety invariant: Σ slack = k·slack = Δ/2 at all times.
        let est = m.global_estimate();
        let delta = 120.0 - est;
        let total_slack = 3.0 * slack_in(&a3, "e1");
        assert!((total_slack - delta / 2.0).abs() < 1e-9);
    }

    #[test]
    fn seq_dedup_ignores_retransmits() {
        let mut m = Monitor::new(cfg(100.0));
        m.on_register("e1", 60_000, 0);
        m.on_report("e1", 0, 40.0, 1, 0.0);
        let before = m.global_estimate();
        // Same seq re-delivered → ignored.
        let a = m.on_report("e1", 0, 40.0, 1, 0.0);
        assert!(a.is_empty());
        assert_eq!(m.global_estimate(), before);
    }

    #[test]
    fn stale_epoch_report_dropped() {
        let mut m = Monitor::new(cfg(100.0));
        m.on_register("e1", 60_000, 120_000); // epoch starts at 120_000
        assert_eq!(m.window_start_ms(), 120_000);
        // A report tagged with the previous epoch is dropped.
        let a = m.on_report("e1", 60_000, 99.0, 1, 0.0);
        assert!(a.is_empty());
        assert_eq!(m.global_estimate(), 0.0);
    }

    #[test]
    fn join_and_leave_mass_accounting() {
        let mut m = Monitor::new(cfg(100.0));
        m.on_register("e1", 60_000, 0);
        m.on_register("e2", 60_000, 0);
        m.on_report("e1", 0, 30.0, 1, 0.0);
        m.on_report("e2", 0, 20.0, 1, 0.0);
        assert_eq!(m.global_estimate(), 50.0);
        // e2 leaves: its 20 stays in the estimate via departed_mass.
        m.on_leave("e2");
        assert_eq!(m.global_estimate(), 50.0);
        assert_eq!(m.edge_count(), 1);
    }

    #[test]
    fn single_edge_grants_no_sampling() {
        // With one edge there is no rate vector to coordinate over → p=1.
        let mut m = Monitor::new(cfg(1000.0));
        m.on_register("e1", 60_000, 0);
        let a = m.on_report("e1", 0, 10.0, 1, 50_000.0);
        assert_eq!(sample_p_in(&a, "e1"), 1.0);
    }

    #[test]
    fn skewed_rates_yield_differentiated_sample_p_above_floor() {
        // Two edges, skewed reported rates: the hot edge should be sampled
        // harder (smaller p) than the quiet edge, and BOTH must sit at/above the
        // ε-derived coupling floor. τ is large so the monitor stays in the grant
        // (not alert) regime while we exercise the allocation.
        let mut m = Monitor::new(cfg(1_000_000.0)); // epsilon 0.05
        m.on_register("e1", 60_000, 0);
        m.on_register("e2", 60_000, 0);
        // e1 hot (100k items/win), e2 quiet (1k/win). Small local values keep us
        // far from τ so no alert fires.
        m.on_report("e1", 0, 1.0, 1, 100_000.0);
        let a = m.on_report("e2", 0, 1.0, 1, 1_000.0);
        let p_hot = sample_p_in(&a, "e1");
        let p_quiet = sample_p_in(&a, "e2");
        assert!(
            p_hot < p_quiet,
            "hot edge p {p_hot} should be < quiet edge p {p_quiet}"
        );
        // Both respect the ε-derived floor for their rate.
        let eps = m.cfg.epsilon;
        assert!(p_hot >= epsilon_sample_floor(eps, 100_000.0) - 1e-9);
        assert!(p_quiet >= epsilon_sample_floor(eps, 1_000.0) - 1e-9);
        assert!(p_hot > 0.0 && p_hot <= 1.0);
        assert!(p_quiet > 0.0 && p_quiet <= 1.0);
        // Slack countdown unaffected: both grants carry the same (positive) slack.
        assert!(slack_in(&a, "e1") > 0.0);
        assert_eq!(slack_in(&a, "e1"), slack_in(&a, "e2"));
    }

    #[test]
    fn epoch_advance_resets_estimate() {
        let mut m = Monitor::new(cfg(100.0));
        m.on_register("e1", 60_000, 0);
        m.on_report("e1", 0, 80.0, 1, 0.0);
        assert_eq!(m.global_estimate(), 80.0);
        // A report for the next epoch advances and resets.
        m.on_report("e1", 60_000, 5.0, 2, 0.0);
        assert_eq!(m.window_start_ms(), 60_000);
        assert_eq!(m.global_estimate(), 5.0);
    }
}
