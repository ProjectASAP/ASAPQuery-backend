//! Storage uses the Planner-owned state implementation.
pub use asap_summary_state::stored_state::delta_apply::*;

#[cfg(test)]
mod tests {
    use super::*;
    use asap_summary_state::stored_state::{SketchEncoding, SketchSampleState};

    #[test]
    fn native_batches_are_not_legacy_sketch_frames() {
        let state = SketchSampleState {
            bytes: vec![],
            encoding: SketchEncoding::NativeBatchV1,
        };
        let samples = [(1000, &state)];
        assert!(cumulative_summary_state(&samples, DeltaSketchKind::Kll { k: 200 }).is_err());
        assert!(per_window_summary_states(&samples, DeltaSketchKind::Kll { k: 200 }).is_err());
    }
}
