//! Shared aggregation vocabulary for configuration and accumulator dispatch.
//! The wire shape combines aggregation type, subtype, and parameters.
//! `AccumulatorSpec` provides a typed representation at conversion boundaries.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Concrete aggregation/sketch type used in precompute configs and accumulator dispatch.
///
/// `Display` outputs the canonical PascalCase name used in YAML/JSON configs.
/// `FromStr` accepts the canonical name plus legacy aliases (e.g. "KLL" → `DatasketchesKLL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationType {
    // ---------- single-population (non-keyed) ----------
    Sum,
    Count,
    Increase,
    Rate,
    Min,
    Max,
    DatasketchesKLL,
    // ---------- multi-population (keyed) ----------
    MultipleSum,
    MultipleIncrease,
    MultipleMin,
    MultipleMax,
    HydraKLL,
    CountMinSketch,
    CountMinSketchWithHeap,
    CountSketch,
    CountSketchWithHeap,
    // ---------- cardinality / set tracking ----------
    HLL,
    UnivMon,
    DDSketch,
    // ---------- legacy config wrapper names ----------
    SingleSubpopulation,
    MultipleSubpopulation,
}

impl AggregationType {
    pub fn as_str(self) -> &'static str {
        match self {
            AggregationType::Sum => "Sum",
            AggregationType::Count => "Count",
            AggregationType::Increase => "Increase",
            AggregationType::Rate => "Rate",
            AggregationType::Min => "Min",
            AggregationType::Max => "Max",
            AggregationType::DatasketchesKLL => "DatasketchesKLL",
            AggregationType::MultipleSum => "MultipleSum",
            AggregationType::MultipleIncrease => "MultipleIncrease",
            AggregationType::MultipleMin => "MultipleMin",
            AggregationType::MultipleMax => "MultipleMax",
            AggregationType::HydraKLL => "HydraKLL",
            AggregationType::CountMinSketch => "CountMinSketch",
            AggregationType::CountMinSketchWithHeap => "CountMinSketchWithHeap",
            AggregationType::CountSketch => "CountSketch",
            AggregationType::CountSketchWithHeap => "CountSketchWithHeap",
            AggregationType::HLL => "HLL",
            AggregationType::UnivMon => "UnivMon",
            AggregationType::DDSketch => "DDSketch",
            AggregationType::SingleSubpopulation => "SingleSubpopulation",
            AggregationType::MultipleSubpopulation => "MultipleSubpopulation",
        }
    }

    /// Returns `true` if this type produces keyed (multi-population) accumulators.
    pub fn is_keyed(self) -> bool {
        matches!(
            self,
            AggregationType::MultipleSubpopulation
                | AggregationType::MultipleSum
                | AggregationType::MultipleIncrease
                | AggregationType::MultipleMin
                | AggregationType::MultipleMax
                | AggregationType::CountMinSketch
                | AggregationType::CountMinSketchWithHeap
                | AggregationType::CountSketch
                | AggregationType::CountSketchWithHeap
                | AggregationType::HydraKLL
        )
    }
}

impl fmt::Display for AggregationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AggregationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // Canonical names
            "Sum" => Ok(AggregationType::Sum),
            "Count" => Ok(AggregationType::Count),
            "Increase" => Ok(AggregationType::Increase),
            "Rate" => Ok(AggregationType::Rate),
            "Min" => Ok(AggregationType::Min),
            "Max" => Ok(AggregationType::Max),
            "DatasketchesKLL" => Ok(AggregationType::DatasketchesKLL),
            "MultipleSum" => Ok(AggregationType::MultipleSum),
            "MultipleIncrease" => Ok(AggregationType::MultipleIncrease),
            "MultipleMin" => Ok(AggregationType::MultipleMin),
            "MultipleMax" => Ok(AggregationType::MultipleMax),
            "HydraKLL" => Ok(AggregationType::HydraKLL),
            "CountMinSketch" => Ok(AggregationType::CountMinSketch),
            "CountMinSketchWithHeap" => Ok(AggregationType::CountMinSketchWithHeap),
            "CountSketch" => Ok(AggregationType::CountSketch),
            "CountSketchWithHeap" => Ok(AggregationType::CountSketchWithHeap),
            "HLL" | "HyperLogLog" => Ok(AggregationType::HLL),
            "UnivMon" => Ok(AggregationType::UnivMon),
            "DDSketch" | "DdSketch" => Ok(AggregationType::DDSketch),
            "SingleSubpopulation" => Ok(AggregationType::SingleSubpopulation),
            "MultipleSubpopulation" => Ok(AggregationType::MultipleSubpopulation),
            // Legacy accumulator-suffixed aliases
            "SumAccumulator" | "SumAggregator" | "sum" => Ok(AggregationType::Sum),
            "IncreaseAccumulator" | "IncreaseAggregator" | "increase" => {
                Ok(AggregationType::Increase)
            }
            "MinAccumulator" | "MinAggregator" | "min" => Ok(AggregationType::Min),
            "MaxAccumulator" | "MaxAggregator" | "max" => Ok(AggregationType::Max),
            "DatasketchesKLLAccumulator" | "KLL" | "kll" | "datasketches_kll" => {
                Ok(AggregationType::DatasketchesKLL)
            }
            "MultipleSumAccumulator" | "multiple_sum" => Ok(AggregationType::MultipleSum),
            "MultipleIncreaseAccumulator" | "multiple_increase" => {
                Ok(AggregationType::MultipleIncrease)
            }
            "MultipleMinAccumulator" | "multiple_min" => Ok(AggregationType::MultipleMin),
            "MultipleMaxAccumulator" | "multiple_max" => Ok(AggregationType::MultipleMax),
            "HydraKllSketchAccumulator" | "hydra_kll" => Ok(AggregationType::HydraKLL),
            "CountMinSketchAccumulator" | "CMS" | "cms" | "count_min_sketch" => {
                Ok(AggregationType::CountMinSketch)
            }
            "CountMinSketchWithHeapAccumulator" => Ok(AggregationType::CountMinSketchWithHeap),
            "CountSketchAccumulator" | "CS" | "cs" | "count_sketch" => {
                Ok(AggregationType::CountSketch)
            }
            "CountSketchWithHeapAccumulator" => Ok(AggregationType::CountSketchWithHeap),
            // Retired names. `MinMax` used to be one accumulator whose
            // direction rode alongside in `aggregationSubType`; the two
            // directions are separate types now, so there is no safe
            // direction to guess here -- resolving a min workload as a
            // max one is silently wrong, not merely imprecise.
            "MinMax"
            | "MinMaxAccumulator"
            | "MinMaxAggregator"
            | "min_max"
            | "MultipleMinMax"
            | "MultipleMinMaxAccumulator"
            | "multiple_min_max" => Err(format!(
                "Retired aggregation type: '{s}' -- min and max are separate types now, \
                 use 'Min'/'Max' (or 'MultipleMin'/'MultipleMax')"
            )),
            _ => Err(format!("Unknown aggregation type: '{s}'")),
        }
    }
}

impl Serialize for AggregationType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AggregationType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}
