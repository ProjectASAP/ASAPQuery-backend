//! Sample blocks: the data plane's encoding of the Planner's
//! compressed-samples state (every sample of one series in one tumbling
//! window). The Planner's executor builds, seals, merges and decodes those
//! states only through its `SampleBlockCodec` trait; every run of the data
//! plane uses [`CompressedSampleBlockCodec`] ([`codec`]), and the view files
//! store its sealed bytes verbatim.
//!
//! A block is codec 0 (Gorilla: delta-of-delta timestamps, XOR values)
//! while it is built; sealing keeps the shorter of codec 0 and codec 1
//! (decimal mantissas, varints, zstd), which holds only short decimals.
//! The format is fixed to the bit in [`CompressedSamples`]'s module.

mod block;

use std::sync::{Arc, OnceLock};

use asap_executor::summary_kernels::{OpenSampleBlock, SampleBlockCodec};
use asap_executor::KernelError;

pub use block::{CompressedSamples, CompressedSamplesFormatError, SampleCodec, SamplesIter};

/// The data plane's sample-block codec: [`CompressedSamples`] blocks.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompressedSampleBlockCodec;

/// The codec every run of the data plane builds compressed-samples states
/// with, and the view files decode them with: one shared instance.
pub fn codec() -> Arc<dyn SampleBlockCodec> {
    static CODEC: OnceLock<Arc<dyn SampleBlockCodec>> = OnceLock::new();
    CODEC
        .get_or_init(|| Arc::new(CompressedSampleBlockCodec))
        .clone()
}

impl SampleBlockCodec for CompressedSampleBlockCodec {
    fn new_block(&self) -> Box<dyn OpenSampleBlock> {
        Box::new(OpenBlock(CompressedSamples::new()))
    }

    fn decode(&self, block: &[u8]) -> Result<Vec<(i64, f64)>, KernelError> {
        Ok(CompressedSamples::from_bytes(block)?.iter().collect())
    }

    fn samples_in(
        &self,
        block: &[u8],
        start_exclusive_ms: i64,
        end_inclusive_ms: i64,
    ) -> Result<Vec<(i64, f64)>, KernelError> {
        Ok(CompressedSamples::from_bytes(block)?
            .samples_in(start_exclusive_ms, end_inclusive_ms)
            .collect())
    }

    fn merge(&self, older: &[u8], newer: &[u8]) -> Result<Vec<u8>, KernelError> {
        let older = CompressedSamples::from_bytes(older)?;
        let newer = CompressedSamples::from_bytes(newer)?;
        Ok(older.merge(&newer).to_bytes())
    }
}

/// A block being built: always codec 0, so appending a newer sample is
/// O(1); sealing re-encodes with the shorter codec.
struct OpenBlock(CompressedSamples);

impl OpenSampleBlock for OpenBlock {
    fn append(&mut self, timestamp_ms: i64, value: f64) {
        self.0.push(timestamp_ms, value);
    }

    fn len(&self) -> usize {
        self.0.len() as usize
    }

    fn memory_bytes(&self) -> usize {
        self.0.encoded_bytes()
    }

    fn seal(&self) -> Vec<u8> {
        self.0.seal().to_bytes()
    }
}
