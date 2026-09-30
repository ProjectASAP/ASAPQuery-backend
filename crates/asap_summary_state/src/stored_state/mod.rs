//! Portable stored-summary payloads and reconstruction, independent of storage engines.
pub mod codec;
pub mod decoders;
pub mod delta_apply;
pub mod native;
pub mod readout;

#[derive(Debug, Clone)]
pub struct SketchSampleState {
    pub bytes: Vec<u8>,
    /// Wire-encoding hint from the OTLP DataPoint's `encoding` field.
    pub encoding: SketchEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SketchEncoding {
    ProtoFull,
    ProtoDelta,
    MsgpackFull,
    MsgpackDelta,
    /// Versioned typed physical output; never a legacy sketch frame.
    NativeBatchV1,
    /// A complete Planner weighted-frequency heap state (sketchlib
    /// `WeightedFrequency` bytes); never a legacy integer heap frame.
    WeightedFrequencyV1,
}

impl SketchEncoding {
    /// A complete window state that needs no earlier base.
    pub fn is_full(self) -> bool {
        matches!(
            self,
            Self::ProtoFull | Self::MsgpackFull | Self::WeightedFrequencyV1
        )
    }

    /// Encoding of a stored sketch state written as one complete window.
    pub fn full_frame_for(state: &dyn crate::AggregateCore) -> Self {
        use asap_physical_operators::summary_kernels::weighted_frequency::WeightedFrequency;
        if state.as_any().is::<WeightedFrequency>() {
            Self::WeightedFrequencyV1
        } else {
            Self::MsgpackFull
        }
    }
}
