//! SHIM: stored UnivMon state.
//!
//! Planner's `UnivMonAccumulator` keeps its sketchlib `UnivMon` private and has
//! no byte form, so stored UnivMon bytes cannot be decoded into it. Delete this
//! module once Planner exposes `UnivMonAccumulator::from_sketch(UnivMon)` and
//! `UnivMonAccumulator::sketch(&self) -> &UnivMon` (or a byte codec).
use asap_physical_operators::{AggregateCore, KernelError};
use asap_sketchlib::{DataInput, UnivMon};
use planner_types::{post_asap::SketchQuery, pre_asap::ColumnRef};

#[derive(Debug, Clone)]
pub struct UnivMonAccumulator {
    inner: UnivMon,
}

impl UnivMonAccumulator {
    pub fn new(
        heap_size: usize,
        rows: usize,
        cols: usize,
        layers: usize,
    ) -> Result<Self, KernelError> {
        if heap_size == 0 || cols == 0 || !(1..=20).contains(&rows) || !(1..=64).contains(&layers) {
            return Err("invalid UnivMon dimensions".into());
        }
        rows.checked_mul(cols)
            .and_then(|n| n.checked_mul(layers))
            .ok_or("UnivMon dimensions overflow")?;
        Ok(Self {
            inner: UnivMon::init_univmon(heap_size, rows, cols, layers),
        })
    }

    /// Each non-NaN sample is one occurrence. Signed zero has one identity.
    pub fn insert_sample(&mut self, value: f64) -> Result<(), KernelError> {
        if value.is_nan() {
            return Ok(());
        }
        self.inner
            .bucket_size
            .checked_add(1)
            .ok_or("UnivMon count overflow")?;
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        self.inner.insert(&DataInput::U64(bits), 1);
        Ok(())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, KernelError> {
        let inner = UnivMon::deserialize_from_bytes(bytes)
            .map_err(|e| format!("invalid UnivMon state: {e}"))?;
        if !inner.accepts_standard_updates() {
            return Err(
                "terminal-mode UnivMon state cannot enter the standard-update accumulator".into(),
            );
        }
        Ok(Self { inner })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, KernelError> {
        Ok(self.inner.serialize_to_bytes()?)
    }

    pub fn dimensions(&self) -> (usize, usize, usize, usize) {
        (
            self.inner.heap_size,
            self.inner.sketch_row,
            self.inner.sketch_col,
            self.inner.layer_size,
        )
    }

    /// Empty the sketch in place, keeping its shape.
    pub fn clear(&mut self) {
        self.inner.free();
    }

    pub fn merge_in_place(&mut self, other: &Self) -> Result<(), KernelError> {
        if self.dimensions() != other.dimensions() {
            return Err("incompatible UnivMon dimensions".into());
        }
        self.inner
            .bucket_size
            .checked_add(other.inner.bucket_size)
            .ok_or("UnivMon count overflow")?;
        self.inner.merge(&other.inner);
        Ok(())
    }
}

impl AggregateCore for UnivMonAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn merge_with(&self, other: &dyn AggregateCore) -> Result<Box<dyn AggregateCore>, KernelError> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or("expected UnivMon state")?;
        let mut merged = self.clone();
        merged.merge_in_place(other)?;
        Ok(Box::new(merged))
    }
    fn estimate(&self, query: &SketchQuery) -> Result<f64, KernelError> {
        Ok(match query {
            SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None,
            } => self.inner.calc_l1(),
            SketchQuery::Cardinality => self.inner.calc_card(),
            SketchQuery::FrequencyL2 => self.inner.calc_l2(),
            SketchQuery::FrequencyEntropy => self.inner.calc_entropy(),
            _ => return Err("unsupported UnivMon readout".into()),
        })
    }
    fn approx_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.inner.layer_size.saturating_mul(
                self.inner
                    .sketch_row
                    .saturating_mul(self.inner.sketch_col)
                    .saturating_mul(16)
                    .saturating_add(self.inner.heap_size.saturating_mul(256)),
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(state: &dyn AggregateCore, query: SketchQuery) -> f64 {
        state.estimate(&query).unwrap()
    }
    fn count() -> SketchQuery {
        SketchQuery::PointCount {
            key: ColumnRef::SampleValue,
            value: None,
        }
    }

    // Duplicate samples affect frequency but not cardinality, including signed zero.
    #[test]
    fn shared_readouts_survive_serialization() {
        let mut state = UnivMonAccumulator::new(32, 5, 1024, 4).unwrap();
        for value in [0.0, -0.0, 2.0, 2.0, f64::NAN] {
            state.insert_sample(value).unwrap();
        }
        let restored = UnivMonAccumulator::from_bytes(&state.to_bytes().unwrap()).unwrap();
        for query in [
            count(),
            SketchQuery::Cardinality,
            SketchQuery::FrequencyL2,
            SketchQuery::FrequencyEntropy,
        ] {
            assert_eq!(read(&state, query.clone()), read(&restored, query));
        }
        assert_eq!(read(&restored, count()), 4.0);
        assert!((read(&restored, SketchQuery::Cardinality) - 2.0).abs() < 0.01);
        assert!((read(&restored, SketchQuery::FrequencyL2) - 8.0f64.sqrt()).abs() < 0.01);
        assert!((read(&restored, SketchQuery::FrequencyEntropy) - 1.0).abs() < 0.01);
    }

    // Terminal-mode serialization is valid sketchlib state but not this accumulator's update domain.
    #[test]
    fn terminal_state_is_rejected_before_ingestion_or_merge() {
        let mut state = UnivMon::init_univmon(4, 3, 16, 2);
        state.fast_insert(&DataInput::U64(1), 1);
        let bytes = state.serialize_to_bytes().unwrap();
        assert!(UnivMonAccumulator::from_bytes(&bytes).is_err());
        state.free();
        assert!(UnivMonAccumulator::from_bytes(&state.serialize_to_bytes().unwrap()).is_ok());
    }

    // Pane merge preserves overlapping keys and clearing removes the previous window.
    #[test]
    fn merge_and_clear_preserve_frequency_semantics() {
        let mut left = UnivMonAccumulator::new(32, 5, 1024, 4).unwrap();
        let mut right = left.clone();
        for value in [1.0, 2.0] {
            left.insert_sample(value).unwrap();
        }
        for value in [2.0, 3.0] {
            right.insert_sample(value).unwrap();
        }
        let merged = left.merge_with(&right).unwrap();
        assert_eq!(read(merged.as_ref(), count()), 4.0);
        assert!((read(merged.as_ref(), SketchQuery::Cardinality) - 3.0).abs() < 0.01);
        left.clear();
        assert_eq!(read(&left, count()), 0.0);
        assert_eq!(read(&left, SketchQuery::FrequencyEntropy), 0.0);
        assert!(left
            .merge_with(&UnivMonAccumulator::new(16, 5, 1024, 4).unwrap())
            .is_err());
    }
}
