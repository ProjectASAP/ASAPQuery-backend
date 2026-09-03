use lazy_static::lazy_static;
use prometheus::{register_counter_vec, CounterVec};

lazy_static! {
    static ref LATE_INPUTS_TOTAL: CounterVec = register_counter_vec!(
        "asap_precompute_late_inputs_total",
        "Precompute inputs handled after their event-time window was late or closed",
        &["action", "input_kind"]
    )
    .expect("late-input counter registration must succeed");
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
}
