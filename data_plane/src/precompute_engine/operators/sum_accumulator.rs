use crate::storage_engines::types::{
    AggregateCore, AggregationType, AuxStats, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use asap_types::Statistic;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumAccumulator {
    pub sum: f64,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0 }
    }

    pub fn with_sum(sum: f64) -> Self {
        Self { sum }
    }

    pub fn update(&mut self, value: f64) {
        self.sum += value;
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let sum = data["sum"]
            .as_f64()
            .ok_or("Missing or invalid 'sum' field")?;
        Ok(Self::with_sum(sum))
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 4 {
            return Err("Buffer too short for f32".into());
        }
        // Python uses struct.pack("<f", self.sum) which is 4-byte little-endian float
        let sum = f32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as f64;
        Ok(Self::with_sum(sum))
    }

    /// Decode the fixed Sum payload produced by the first-class Sum
    /// AggregationType path (asap-precompute-go's SumWrapper): float64 sum
    /// (little-endian) followed by uint64 count (little-endian), 16 bytes.
    ///
    /// Sum is an aggregation, NOT a sketch, so this deliberately does NOT
    /// depend on the sketchlib sketch-envelope proto — the payload is a small
    /// self-contained fixed layout. It decodes into the SAME
    /// `AggregationType::Sum` accumulator as a plain-OTLP Sum, so the SumAgg
    /// envelope and a plain Sum land on one identity (`exact_agg:Sum`) with no
    /// new SketchAlgorithm. `count` is decoded but not retained
    /// (SumAccumulator tracks the scalar sum only; Sum is never sample_p-thinned
    /// so no 1/p rescale is needed).
    pub fn from_sum_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 16 {
            return Err(format!("Sum payload too short: {} bytes (want 16)", buffer.len()).into());
        }
        let sum = f64::from_le_bytes(buffer[0..8].try_into().unwrap());
        // count = u64::from_le_bytes(buffer[8..16]) — decoded position documented
        // but not retained by the scalar-sum accumulator.
        Ok(Self::with_sum(sum))
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for SumAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "sum": self.sum
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Match Python's struct.pack("<f", self.sum) - 4-byte little-endian float
        (self.sum as f32).to_le_bytes().to_vec()
    }
}

impl AggregateCore for SumAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "SumAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a SumAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge SumAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to SumAccumulator
        let other_sum = other
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .ok_or("Failed to downcast to SumAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_sum.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::Sum
    }

    fn approx_memory_bytes(&self) -> usize {
        // Single f64 + struct overhead.
        std::mem::size_of::<Self>()
    }

    fn aux_stats(&self) -> AuxStats {
        // SumAccumulator tracks exactly one scalar — the sum.
        // Count/min/max aren't retained by this type.
        AuxStats {
            sum: Some(self.sum),
            ..AuxStats::empty()
        }
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        statistic: asap_types::Statistic,
        _key: &Option<crate::KeyByLabelValues>,
        _query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::storage_engines::types::SingleSubpopulationAggregate;
        self.query(statistic, None)
    }
}

impl SingleSubpopulationAggregate for SumAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // SumAccumulator doesn't use query_kwargs, assert it's None
        if query_kwargs.is_some() {
            return Err("SumAccumulator does not support query parameters".into());
        }

        match statistic {
            Statistic::Sum | Statistic::Count => Ok(self.sum),
            _ => Err(format!("Unsupported statistic in SumAccumulator: {statistic:?}").into()),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

// Factory implementation for merging
pub struct SumAccumulatorFactory;

impl SingleSubpopulationAggregateFactory for SumAccumulatorFactory {
    fn merge_accumulators(
        &self,
        accumulators: Vec<Box<dyn SingleSubpopulationAggregate>>,
    ) -> Result<Box<dyn SingleSubpopulationAggregate>, Box<dyn std::error::Error + Send + Sync>>
    {
        let mut total_sum = 0.0;

        for acc in accumulators {
            if acc.type_name() != "SumAccumulator" {
                return Err("Cannot merge different accumulator types".into());
            }
            let sum_value = acc.query(Statistic::Sum, None)?;
            total_sum += sum_value;
        }

        Ok(Box::new(SumAccumulator::with_sum(total_sum)))
    }

    fn create_default(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(SumAccumulator::new())
    }
}

impl MergeableAccumulator<SumAccumulator> for SumAccumulator {
    fn merge_accumulators(
        accumulators: Vec<SumAccumulator>,
    ) -> Result<SumAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        let total_sum = accumulators.iter().map(|acc| acc.sum).sum();
        Ok(SumAccumulator::with_sum(total_sum))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_accumulator_creation() {
        let acc = SumAccumulator::new();
        assert_eq!(acc.sum, 0.0);

        let acc2 = SumAccumulator::with_sum(42.5);
        assert_eq!(acc2.sum, 42.5);
    }

    #[test]
    fn test_sum_accumulator_update() {
        let mut acc = SumAccumulator::new();
        acc.update(10.0);
        acc.update(20.0);
        assert_eq!(acc.sum, 30.0);
    }

    #[test]
    fn test_sum_accumulator_query() {
        let acc = SumAccumulator::with_sum(42.0);

        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Sum, None).unwrap(),
            42.0
        );
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Count, None).unwrap(),
            42.0
        );

        assert!(crate::SingleSubpopulationAggregate::query(&acc, Statistic::Min, None).is_err());
        // SumAccumulator is a single subpopulation accumulator, doesn't need key-based queries
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Sum, None).unwrap(),
            42.0
        );
    }

    #[test]
    fn test_sum_accumulator_merge() {
        let acc1 = SumAccumulator::with_sum(10.0);
        let acc2 = SumAccumulator::with_sum(20.0);
        let acc3 = SumAccumulator::with_sum(30.0);

        let merged =
            <SumAccumulator as MergeableAccumulator<SumAccumulator>>::merge_accumulators(vec![
                acc1, acc2, acc3,
            ])
            .unwrap();
        assert_eq!(merged.sum, 60.0);
    }

    #[test]
    fn test_sum_accumulator_serialization() {
        let acc = SumAccumulator::with_sum(42.5);

        // Test JSON serialization
        let json = acc.serialize_to_json();
        let deserialized = SumAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(acc.sum, deserialized.sum);

        // Test byte serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes = SumAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(acc.sum, deserialized_bytes.sum);
    }

    #[test]
    fn test_trait_object() {
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(42.0));

        assert_eq!(acc.type_name(), "SumAccumulator");
    }

    #[test]
    fn from_sum_bytes_decodes_go_sum_payload() {
        // GOLDEN: the 16-byte payload asap-precompute-go's
        // SumWrapper{10,20,30,40}.Snapshot() emits — float64 sum (LE) followed
        // by uint64 count (LE), sum=100, count=4. Proves the Rust backend
        // decodes the first-class Sum payload the Go agent produces
        // (cross-language wire parity, no sketchlib proto dependency).
        let go_bytes: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // 100.0 f64 LE
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 4 u64 LE
        ];
        let acc = SumAccumulator::from_sum_bytes(go_bytes).expect("decode Go Sum payload");
        assert_eq!(acc.sum, 100.0, "decoded Go SumWrapper payload sum");
    }

    #[test]
    fn from_sum_bytes_rejects_short_payload() {
        // A short buffer is rejected (the ingest path then skips the point).
        assert!(SumAccumulator::from_sum_bytes(&[]).is_err());
        assert!(SumAccumulator::from_sum_bytes(&[0u8; 8]).is_err());
    }

    #[test]
    fn aux_stats_exposes_sum_only() {
        let acc = SumAccumulator::with_sum(123.5);
        let aux = acc.aux_stats();
        assert_eq!(aux.sum, Some(123.5));
        assert_eq!(aux.count, None);
        assert_eq!(aux.min, None);
        assert_eq!(aux.max, None);
    }

    #[test]
    fn aux_stats_try_answer_on_sum_statistic() {
        use asap_types::Statistic;
        let acc = SumAccumulator::with_sum(42.0);
        // Sum statistic is covered by aux without deserialising.
        assert_eq!(acc.aux_stats().try_answer(Statistic::Sum), Some(42.0));
        // Count is not tracked by SumAccumulator.
        assert_eq!(acc.aux_stats().try_answer(Statistic::Count), None);
    }
}
