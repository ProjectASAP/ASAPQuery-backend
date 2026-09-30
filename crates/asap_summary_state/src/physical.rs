//! Conversion between stored summary state and Planner physical state.
//!
//! Planner physical operators accept only their own in-memory kernels. Stored
//! kernels carry storage-only fields (for example edge `sample_p`), so only the
//! families Planner can bind as typed inputs convert, and only when those
//! fields are neutral.
use crate::summary_kernels as stored;
use crate::AggregateCore as StoredState;
use asap_physical_operators::summary_kernels as physical;
use asap_physical_operators::AggregateCore as PhysicalState;
use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Bind a stored state as a Planner physical input.
///
/// A stored `SumAccumulator` becomes Planner's unkeyed exact Sum state, which
/// is the only exact representation Planner operators accept.
pub fn to_physical(state: &dyn StoredState) -> Result<Arc<dyn PhysicalState>, Error> {
    let any = state.as_any();
    if let Some(s) = any.downcast_ref::<stored::DDSketchAccumulator>() {
        unsampled(s.sample_p)?;
        return Ok(Arc::new(physical::DDSketchAccumulator {
            inner: s.inner.clone(),
        }));
    }
    if let Some(s) = any.downcast_ref::<stored::HllSketchAccumulator>() {
        unsampled(s.sample_p)?;
        return Ok(Arc::new(physical::HllSketchAccumulator {
            inner: s.inner.clone(),
        }));
    }
    if let Some(s) = any.downcast_ref::<stored::DatasketchesKLLAccumulator>() {
        return Ok(Arc::new(physical::DatasketchesKLLAccumulator {
            inner: s.inner.clone(),
        }));
    }
    if let Some(s) = any.downcast_ref::<stored::exact::ExactAccumulator>() {
        return Ok(Arc::new(exact_to_physical(s)?));
    }
    if let Some(s) = any.downcast_ref::<stored::weighted_frequency::WeightedFrequency>() {
        return Ok(Arc::new(s.0.clone()));
    }
    if let Some(s) = any.downcast_ref::<stored::SumAccumulator>() {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType};
        let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let mut exact = physical::exact::ExactAccumulator::new(family, false)?;
        exact.update(None, s.sum, 0);
        return Ok(Arc::new(exact));
    }
    Err(format!("{} has no Planner physical state", state.type_name()).into())
}

/// Keep a Planner physical output in the stored kernel family.
pub fn from_physical(state: &dyn PhysicalState) -> Result<Box<dyn StoredState>, Error> {
    let any = state.as_any();
    if let Some(s) = any.downcast_ref::<physical::DDSketchAccumulator>() {
        return Ok(Box::new(stored::DDSketchAccumulator {
            inner: s.inner.clone(),
            sample_p: 1.0,
        }));
    }
    if let Some(s) = any.downcast_ref::<physical::HllSketchAccumulator>() {
        return Ok(Box::new(stored::HllSketchAccumulator {
            inner: s.inner.clone(),
            sample_p: 1.0,
        }));
    }
    if let Some(s) = any.downcast_ref::<physical::DatasketchesKLLAccumulator>() {
        return Ok(Box::new(stored::DatasketchesKLLAccumulator {
            inner: s.inner.clone(),
        }));
    }
    if let Some(s) = any.downcast_ref::<physical::weighted_frequency::WeightedFrequency>() {
        return Ok(Box::new(stored::weighted_frequency::WeightedFrequency(
            s.clone(),
        )));
    }
    if let Some(s) = any.downcast_ref::<physical::exact::ExactAccumulator>() {
        return Ok(Box::new(
            stored::exact::ExactAccumulator::deserialize_from_bytes(&rmp_serde::to_vec_named(s)?)?,
        ));
    }
    Err("Planner physical state has no stored kernel".into())
}

/// Stored kernel identity of a Planner physical state.
pub fn aggregation_type(state: &dyn PhysicalState) -> Result<crate::AggregationType, Error> {
    use crate::AggregationType as T;
    let any = state.as_any();
    Ok(if any.is::<physical::DDSketchAccumulator>() {
        T::DDSketch
    } else if any.is::<physical::HllSketchAccumulator>() {
        T::HLL
    } else if any.is::<physical::DatasketchesKLLAccumulator>() {
        T::DatasketchesKLL
    } else {
        from_physical(state)?.get_accumulator_type()
    })
}

/// Planner's weighted frequency state is serde-transparent over the sketchlib
/// kernel, which owns its persisted byte form.
pub(crate) fn frequency_kernel(
    state: &physical::weighted_frequency::WeightedFrequency,
) -> Result<asap_sketchlib::WeightedFrequency, Error> {
    Ok(rmp_serde::from_slice(&rmp_serde::to_vec(state)?)?)
}

/// Both exact states share one serde shape; the stored decoder checks that the
/// payload matches its declared family.
fn exact_to_physical(
    state: &stored::exact::ExactAccumulator,
) -> Result<physical::exact::ExactAccumulator, Error> {
    use crate::SerializableToSink;
    Ok(rmp_serde::from_slice(&state.serialize_to_bytes())?)
}

fn unsampled(sample_p: f64) -> Result<(), Error> {
    if sample_p == 1.0 {
        Ok(())
    } else {
        Err("edge-sampled sketches have no Planner physical state".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyByLabelValues, SerializableToSink, Statistic};
    use planner_types::post_asap::{ExactKind, ExactParams, SketchQuery, SummaryFamilyType};

    // Sketches keep their estimates in both directions.
    #[test]
    fn sketches_convert_both_ways() {
        let mut dd = stored::DDSketchAccumulator::new(0.01);
        let mut kll = stored::DatasketchesKLLAccumulator::new(200);
        for v in 1..=100 {
            dd.inner.update(f64::from(v));
            kll.update(f64::from(v));
        }
        let q = SketchQuery::Quantile { q: 0.5 };
        for state in [&dd as &dyn StoredState, &kll] {
            let physical = to_physical(state).unwrap();
            let median = physical.estimate(&q).unwrap();
            assert!((median - 50.0).abs() <= 2.0, "{median}");
            let back = from_physical(physical.as_ref()).unwrap();
            assert_eq!(back.serialize_to_bytes(), state.serialize_to_bytes());
        }
    }

    // Edge-sampled sketches must not become unscaled Planner inputs.
    #[test]
    fn sampled_sketch_is_rejected() {
        let mut dd = stored::DDSketchAccumulator::new(0.01);
        dd.sample_p = 0.5;
        assert!(to_physical(&dd).is_err());
    }

    // Exact state keeps family and value; a stored Sum binds as Planner exact Sum.
    #[test]
    fn exact_and_sum_bind_as_planner_exact_state() {
        let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let mut exact = stored::exact::ExactAccumulator::new(family.clone(), false).unwrap();
        exact.update(None, 4.5, 10);
        let physical = to_physical(&exact).unwrap();
        let read = |state: &Arc<dyn PhysicalState>| {
            state
                .as_any()
                .downcast_ref::<physical::exact::ExactAccumulator>()
                .unwrap()
                .readout(Statistic::Sum, None, None::<&KeyByLabelValues>)
                .unwrap()
        };
        assert_eq!(read(&physical), Some(4.5));
        let back = from_physical(physical.as_ref()).unwrap();
        assert_eq!(back.serialize_to_bytes(), exact.serialize_to_bytes());

        let sum = stored::SumAccumulator::with_sum(7.0);
        assert_eq!(read(&to_physical(&sum).unwrap()), Some(7.0));
    }

    // Heap state keeps its bytes and kernel identity through the stored form.
    #[test]
    fn weighted_frequency_converts_both_ways() {
        use asap_physical_operators::values::Value;
        use physical::weighted_frequency::{FrequencyAlgorithm, WeightedFrequency};
        let mut state = WeightedFrequency::new(FrequencyAlgorithm::Cms, 64, 5, 8).unwrap();
        state.update(&[Value::Utf8("a".into())], 0.5).unwrap();
        let stored = from_physical(&state).unwrap();
        assert_eq!(
            stored.get_accumulator_type(),
            crate::AggregationType::CountMinSketchWithHeap
        );
        assert_eq!(
            aggregation_type(&state).unwrap(),
            crate::AggregationType::CountMinSketchWithHeap
        );
        let back = to_physical(stored.as_ref()).unwrap();
        let back = back.as_any().downcast_ref::<WeightedFrequency>().unwrap();
        assert_eq!(
            frequency_kernel(back).unwrap().to_bytes(),
            frequency_kernel(&state).unwrap().to_bytes()
        );
    }

    // Stored-only kernels have no Planner physical representation.
    #[test]
    fn stored_only_kernels_do_not_convert() {
        assert!(to_physical(&stored::MinAccumulator::new()).is_err());
    }
}
