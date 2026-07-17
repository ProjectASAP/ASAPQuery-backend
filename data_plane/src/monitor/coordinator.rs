//! The per-edge coordinated-sampling state machine.
//!
//! Historically this module also ran the Cormode–Muthukrishnan–Yi distributed
//! functional-monitoring countdown (global-threshold alerting: register →
//! grant `(slack, sample_p)` → countdown → report → alert). That alerting half
//! is RETIRED as of the 2026-07 insert-time-GOS redesign
//! (`ASAPCollector/docs/design-gos-unified-edge-telemetry.md` §11): "Alerting
//! and any other query-time decision is made entirely at the backend against
//! [the reconstructed sketch state]; the edge no longer makes alerting
//! decisions itself." An edge/collector must never be the thing that fires an
//! alert — that decision now belongs entirely to query-time reads of the
//! backend's synced state, not to this streaming protocol.
//!
//! What's left, and what changed: `obsCount`/rate-tracking (feeding the
//! coordinated-sampling `p_i` grant) is a genuinely separate concern from
//! alerting and is PRESERVED — but it no longer piggybacks on the alerting
//! trigger. The whole-sketch ε-floor `p_i = 1/(1 + ε²·rate_i)`
//! ([`super::sampling_alloc::epsilon_sample_floor`]) is a PURE PER-EDGE
//! function of that edge's own reported rate — it never depended on other
//! edges' state or on a global estimate/τ, so retiring the countdown that used
//! to gate reporting doesn't remove anything the sampling law needed. An edge
//! now reports its rate on its own periodic cadence (see the Go edge's
//! `Engine.Observe` — decoupled from any value/slack threshold), and the
//! coordinator answers that ONE edge with its own fresh grant immediately —
//! no more re-broadcasting to every registered edge on every report (that
//! fan-out existed only because slack sizing depended on the full edge set).
//!
//! `MonitorConfig.tau` is no longer consulted here (alerting is gone); it
//! stays in the struct only because it still rides the shared streaming-config
//! `monitors:` schema alongside `epsilon`/`window_ms` — trimming it is a
//! config-schema change, not a coordinator behavior change, and out of scope
//! here.

use std::collections::HashMap;

use super::sampling_alloc::epsilon_sample_floor;

/// Which readout of a series' sketch a monitor's rate is scoped to. Retained
/// for identity/config-schema compatibility (a monitor is still keyed by
/// `(agg_id, key)`, and `key` is only meaningful for `CmsPoint`) even though
/// no functional-specific THRESHOLDING happens here anymore.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Functional {
    #[default]
    Sum,
    CmsPoint,
    LinearBuckets,
}

impl Functional {
    /// Parse the pushed-config functional string (`streaming_config` /
    /// `emit/monitor.rs` use the same names). Unknown ⇒ `Sum`.
    pub fn from_name(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "cms_point" | "cms" => Functional::CmsPoint,
            "linear_buckets" | "linear" => Functional::LinearBuckets,
            _ => Functional::Sum,
        }
    }
}

/// Static configuration for one monitor, sourced from the streaming-config
/// `monitors:` section.
#[derive(Clone, Debug, PartialEq)]
pub struct MonitorConfig {
    pub agg_id: u64,
    pub key: Vec<u8>,
    /// No longer consulted (alerting retired) — see the module doc comment.
    pub tau: f64,
    /// The ε-floor tolerance feeding [`epsilon_sample_floor`].
    pub epsilon: f64,
    pub window_ms: u64,
    pub functional: Functional,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            agg_id: 0,
            key: Vec::new(),
            tau: 0.0,
            epsilon: 0.05,
            window_ms: 0,
            functional: Functional::Sum,
        }
    }
}

/// An action the coordinator wants the transport to perform. `Alert` is gone —
/// this coordinator no longer makes alerting decisions (see module docs).
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Send this ONE edge its freshly-computed coordinated-sampling grant, in
    /// direct response to its rate report. `sample_p` is the whole-sketch
    /// ε-floor for the edge's own just-reported rate — independent of every
    /// other edge, so (unlike the retired countdown) this never needs to
    /// re-grant the rest of the edge set. `local_slack` is always 0: kept on
    /// the wire message for `SlackGrant` compatibility, but carries no
    /// meaning post-retirement — the edge no longer gates anything on it.
    Grant {
        edge_id: String,
        round: u64,
        local_slack: f64,
        window_start_ms: u64,
        sample_p: f64,
    },
}

#[derive(Clone, Debug, Default)]
struct EdgeView {
    /// Last `rate` (items/window) the edge reported; 0 = unknown ⇒ no
    /// sampling for this edge. The only per-edge state left — there is no
    /// more `known_value`/global estimate to track once alerting is gone.
    rate: f64,
}

/// One monitor's coordinator state for the current epoch: which edges are
/// registered and each one's last-reported rate.
pub struct Monitor {
    cfg: MonitorConfig,
    window_start_ms: u64,
    round: u64,
    edges: HashMap<String, EdgeView>,
}

impl Monitor {
    pub fn new(cfg: MonitorConfig) -> Self {
        Self {
            cfg,
            window_start_ms: 0,
            round: 0,
            edges: HashMap::new(),
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

    /// The coordinated-sampling grant this monitor would currently compute for
    /// `edge_id`, or `None` if the edge hasn't reported a rate yet
    /// (test/observability — e.g. the cross-language e2e harness polls this to
    /// confirm a real grant reached an edge, without needing a callback hook
    /// into `dispatch`).
    pub fn sample_p_for_edge(&self, edge_id: &str) -> Option<f64> {
        let edge = self.edges.get(edge_id)?;
        if edge.rate <= 0.0 {
            return None;
        }
        Some(epsilon_sample_floor(self.cfg.epsilon, edge.rate))
    }

    /// Ensure the monitor is on the epoch for `window_start`. Advancing resets
    /// every edge's rate (a new window starts a fresh rate measurement).
    /// Returns false if the timestamp is for a stale (already-closed) epoch,
    /// in which case the caller should drop the event.
    fn ensure_epoch(&mut self, window_start: u64) -> bool {
        if window_start == self.window_start_ms {
            return true;
        }
        if window_start > self.window_start_ms {
            self.window_start_ms = window_start;
            self.round = 0;
            for e in self.edges.values_mut() {
                *e = EdgeView::default();
            }
            return true;
        }
        false // stale epoch
    }

    /// Register (or refresh) an edge for the epoch it reports as
    /// `window_start_ms`. Adds the edge to the membership set. No grant is
    /// returned here — an edge is safely unsampled (p=1) until its first rate
    /// report, and unlike the retired countdown, a new registration doesn't
    /// need to perturb any other edge's already-granted `p`.
    pub fn on_register(
        &mut self,
        edge_id: &str,
        epoch_window_ms: u64,
        window_start_ms: u64,
    ) -> Vec<Action> {
        // Alignment guard: a mismatched window size would silently corrupt
        // epoch bookkeeping. Refuse the registration (no grants) and let the
        // caller log/close the stream.
        if epoch_window_ms != self.cfg.window_ms {
            return Vec::new();
        }
        if !self.ensure_epoch(window_start_ms) {
            return Vec::new();
        }
        self.edges.entry(edge_id.to_string()).or_default();
        Vec::new()
    }

    /// Handle an edge's periodic rate report: record the rate and answer that
    /// SAME edge with its freshly-computed coordinated-sampling grant.
    /// Stale-epoch and unknown-edge reports are dropped.
    pub fn on_report(&mut self, edge_id: &str, window_start_ms: u64, rate: f64) -> Vec<Action> {
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
        // Ignore non-positive = unknown; keeps the last known-good rate.
        if rate > 0.0 {
            edge.rate = rate;
        }
        self.round += 1;
        let p = if edge.rate > 0.0 {
            epsilon_sample_floor(self.cfg.epsilon, edge.rate)
        } else {
            1.0
        };
        vec![Action::Grant {
            edge_id: edge_id.to_string(),
            round: self.round,
            local_slack: 0.0,
            window_start_ms: self.window_start_ms,
            sample_p: p,
        }]
    }

    /// An edge left (stream closed). Just drops it from membership — there is
    /// no mass/estimate to fold anywhere anymore.
    pub fn on_leave(&mut self, edge_id: &str) {
        self.edges.remove(edge_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(epsilon: f64) -> MonitorConfig {
        MonitorConfig {
            agg_id: 1,
            key: Vec::new(),
            tau: 0.0,
            epsilon,
            window_ms: 60_000,
            ..Default::default()
        }
    }

    fn grant_for<'a>(actions: &'a [Action], edge: &str) -> Option<&'a Action> {
        actions
            .iter()
            .find(|a| matches!(a, Action::Grant { edge_id, .. } if edge_id == edge))
    }

    fn sample_p_in(actions: &[Action], edge: &str) -> f64 {
        match grant_for(actions, edge) {
            Some(Action::Grant { sample_p, .. }) => *sample_p,
            _ => panic!("no grant for {edge}"),
        }
    }

    #[test]
    fn registration_grants_nothing() {
        // Unlike the retired countdown, registering does not perturb any
        // edge's grant — an edge stays unsampled (p=1, the edge-side default)
        // until it actually reports a rate.
        let mut m = Monitor::new(cfg(0.05));
        let a = m.on_register("e1", 60_000, 0);
        assert!(a.is_empty());
        assert_eq!(m.edge_count(), 1);
        assert_eq!(m.round(), 0);
    }

    #[test]
    fn alignment_guard_rejects_mismatched_window() {
        let mut m = Monitor::new(cfg(0.05));
        let a = m.on_register("e1", 30_000, 0); // wrong window size
        assert!(a.is_empty());
        assert_eq!(m.edge_count(), 0);
    }

    #[test]
    fn report_grants_only_the_reporting_edge() {
        // Unlike the retired countdown (which re-broadcast to every edge on
        // any report, since slack depended on the full edge set), a report
        // now answers ONLY the reporting edge — the sampling law is per-edge.
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        m.on_register("e2", 60_000, 0);
        let a = m.on_report("e1", 0, 100_000.0);
        assert_eq!(a.len(), 1, "report should grant only the reporting edge");
        assert!(grant_for(&a, "e1").is_some());
        assert!(grant_for(&a, "e2").is_none());
    }

    #[test]
    fn sample_p_matches_epsilon_floor_of_reported_rate() {
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        let a = m.on_report("e1", 0, 100_000.0);
        let want = epsilon_sample_floor(0.05, 100_000.0);
        assert!((sample_p_in(&a, "e1") - want).abs() < 1e-12);
    }

    #[test]
    fn single_edge_is_sampled_like_any_other() {
        // The retired countdown special-cased "<2 edges ⇒ p=1" purely to keep
        // its alert-fire safety proof correct under sampling noise. With
        // alerting gone, a lone high-rate edge is sampled exactly like it
        // would be in a larger edge set — the ε-floor law never depended on
        // edge count.
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        let a = m.on_report("e1", 0, 100_000.0);
        let p = sample_p_in(&a, "e1");
        assert!(p < 1.0, "a lone high-rate edge should still be sampled, got p={p}");
    }

    #[test]
    fn unknown_rate_yields_unsampled_grant() {
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        let a = m.on_report("e1", 0, 0.0); // rate unknown/non-positive
        assert_eq!(sample_p_in(&a, "e1"), 1.0);
    }

    #[test]
    fn report_before_register_is_dropped() {
        let mut m = Monitor::new(cfg(0.05));
        let a = m.on_report("e1", 0, 100_000.0);
        assert!(a.is_empty(), "unregistered edge's report must be dropped");
    }

    #[test]
    fn stale_epoch_report_dropped() {
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 120_000); // epoch starts at 120_000
        assert_eq!(m.window_start_ms(), 120_000);
        let a = m.on_report("e1", 60_000, 100_000.0); // previous epoch
        assert!(a.is_empty());
    }

    #[test]
    fn epoch_advance_resets_rate() {
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        m.on_report("e1", 0, 100_000.0);
        // A report for the next epoch advances and resets the rate: an
        // unknown (0) rate on the fresh epoch yields an unsampled grant.
        let a = m.on_report("e1", 60_000, 0.0);
        assert_eq!(m.window_start_ms(), 60_000);
        assert_eq!(sample_p_in(&a, "e1"), 1.0);
    }

    #[test]
    fn leave_drops_membership() {
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        m.on_register("e2", 60_000, 0);
        assert_eq!(m.edge_count(), 2);
        m.on_leave("e2");
        assert_eq!(m.edge_count(), 1);
    }

    #[test]
    fn rate_stays_last_known_good_on_non_positive_report() {
        let mut m = Monitor::new(cfg(0.05));
        m.on_register("e1", 60_000, 0);
        m.on_report("e1", 0, 100_000.0);
        // A subsequent non-positive rate report doesn't clobber the last
        // known-good rate.
        let a = m.on_report("e1", 0, 0.0);
        let want = epsilon_sample_floor(0.05, 100_000.0);
        assert!((sample_p_in(&a, "e1") - want).abs() < 1e-12);
    }
}
