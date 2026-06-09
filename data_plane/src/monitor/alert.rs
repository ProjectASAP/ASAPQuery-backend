//! Global-threshold alert emission. Reuses the control-plane violation sink
//! (`OnViolationFn`) rather than inventing a new egress, so a crossed CDM
//! threshold flows through the same path operators already watch for
//! bandwidth/accuracy/cpu violations.

use std::sync::Arc;

use control_plane::monitor::{Violation, ViolationKind};

/// A sink for global-threshold-crossed alerts. The default wiring is the same
/// `OnViolationFn` the control-plane scraper uses.
pub type AlertSink = Arc<dyn Fn(Violation) + Send + Sync>;

/// Build a `Violation` for a monitor whose global aggregate crossed τ.
pub fn global_threshold_violation(
    monitor_id: impl Into<String>,
    global_estimate: f64,
    tau: f64,
) -> Violation {
    Violation {
        agent_id: monitor_id.into(),
        kind: ViolationKind::GlobalThresholdCrossed,
        observed: global_estimate,
        threshold: tau,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn routes_through_violation_sink() {
        let seen: Arc<Mutex<Vec<Violation>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let sink: AlertSink = Arc::new(move |v| seen2.lock().unwrap().push(v));
        sink(global_threshold_violation("agg:1/sum", 98.0, 100.0));
        let got = seen.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, ViolationKind::GlobalThresholdCrossed);
        assert_eq!(got[0].observed, 98.0);
        assert_eq!(got[0].threshold, 100.0);
    }
}
