//! Stored form of Planner's weighted frequency state. It is read only through
//! typed heap rows, never through a scalar statistic.
use crate::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink, Statistic};
pub use asap_physical_operators::summary_kernels::weighted_frequency::{
    FrequencyAlgorithm, WeightedFrequency as PhysicalWeightedFrequency,
};
use asap_physical_operators::AggregateCore as PhysicalState;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct WeightedFrequency(pub PhysicalWeightedFrequency);

impl SerializableToSink for WeightedFrequency {
    fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.0).expect("finite validated frequency state")
    }
    fn serialize_to_bytes(&self) -> Vec<u8> {
        crate::physical::frequency_kernel(&self.0)
            .expect("weighted frequency kernel encoding")
            .to_bytes()
    }
}

impl AggregateCore for WeightedFrequency {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }
    fn type_name(&self) -> &'static str {
        "WeightedFrequency"
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
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or("weighted frequency state type mismatch")?;
        let merged = self.0.merge_with(&other.0)?;
        let merged = merged
            .as_any()
            .downcast_ref::<PhysicalWeightedFrequency>()
            .ok_or("weighted frequency merge changed state type")?;
        Ok(Box::new(Self(merged.clone())))
    }
    fn get_accumulator_type(&self) -> AggregationType {
        match crate::physical::frequency_kernel(&self.0)
            .expect("weighted frequency kernel encoding")
            .algorithm()
        {
            FrequencyAlgorithm::Cms => AggregationType::CountMinSketchWithHeap,
            FrequencyAlgorithm::CountSketch => AggregationType::CountSketchWithHeap,
        }
    }
    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        None
    }
    fn query_statistic(
        &self,
        _: Statistic,
        _: &Option<KeyByLabelValues>,
        _: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("weighted frequency uses typed row readout".into())
    }
    fn approx_memory_bytes(&self) -> usize {
        self.0.approx_memory_bytes()
    }
}
