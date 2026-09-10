use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_int_counter, CounterVec, IntCounter};

lazy_static! {
    static ref LATE_INPUTS_TOTAL: CounterVec = register_counter_vec!(
        "asap_precompute_late_inputs_total",
        "Precompute inputs handled after their event-time window was late or closed",
        &["action", "input_kind"]
    )
    .expect("late-input counter registration must succeed");
    static ref ACCEPTED_SAMPLES_TOTAL: IntCounter = register_int_counter!(
        "asap_precompute_accepted_samples_total",
        "Unique input samples accepted and queued for precompute"
    )
    .expect("accepted-sample counter registration must succeed");
    static ref PROCESSED_UPDATES_TOTAL: IntCounter = register_int_counter!(
        "asap_precompute_processed_updates_total",
        "Sample updates dequeued by precompute workers"
    )
    .expect("processed-update counter registration must succeed");
    static ref MATERIALIZED_OUTPUTS_TOTAL: IntCounter = register_int_counter!(
        "asap_precompute_materialized_outputs_total",
        "Completed summary outputs successfully installed in SummaryStore"
    )
    .expect("materialized-output counter registration must succeed");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThroughputTotals {
    pub accepted_samples: u64,
    pub processed_updates: u64,
    pub materialized_outputs: u64,
}

impl ThroughputTotals {
    pub fn since(self, previous: Self) -> Self {
        Self {
            accepted_samples: self
                .accepted_samples
                .saturating_sub(previous.accepted_samples),
            processed_updates: self
                .processed_updates
                .saturating_sub(previous.processed_updates),
            materialized_outputs: self
                .materialized_outputs
                .saturating_sub(previous.materialized_outputs),
        }
    }
}

pub fn throughput_totals() -> ThroughputTotals {
    ThroughputTotals {
        accepted_samples: ACCEPTED_SAMPLES_TOTAL.get(),
        processed_updates: PROCESSED_UPDATES_TOTAL.get(),
        materialized_outputs: MATERIALIZED_OUTPUTS_TOTAL.get(),
    }
}

pub fn record_accepted_samples(count: u64) {
    ACCEPTED_SAMPLES_TOTAL.inc_by(count);
}

pub fn record_processed_updates(count: u64) {
    PROCESSED_UPDATES_TOTAL.inc_by(count);
}

pub fn record_materialized_outputs(count: u64) {
    MATERIALIZED_OUTPUTS_TOTAL.inc_by(count);
}

pub fn record_late_input(action: &'static str, input_kind: &'static str) {
    LATE_INPUTS_TOTAL
        .with_label_values(&[action, input_kind])
        .inc();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_input_actions_are_exported() {
        record_late_input("drop", "raw_sample");
        let family = prometheus::gather()
            .into_iter()
            .find(|family| family.get_name() == "asap_precompute_late_inputs_total")
            .expect("late-input counter must be registered");
        assert!(family.get_metric().iter().any(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.get_name() == "action" && label.get_value() == "drop")
        }));
    }

    #[test]
    fn throughput_observation_is_monotonic_and_non_destructive() {
        let before = throughput_totals();
        record_accepted_samples(7);
        record_processed_updates(5);
        record_materialized_outputs(2);
        let after = throughput_totals();
        assert_eq!(after, throughput_totals());
        assert_eq!(
            after.since(before),
            ThroughputTotals {
                accepted_samples: 7,
                processed_updates: 5,
                materialized_outputs: 2,
            }
        );
    }
}
