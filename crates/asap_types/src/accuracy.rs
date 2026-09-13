//! Accuracy wire metadata projected from ASAPPlanner guarantees.
//! Formula ownership stays in ASAPPlanner; unknown confidence remains explicit.

use serde::{Deserialize, Serialize};

/// Interpretation of the reported epsilon; serialized identically across planners and serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccuracyKind {
    Uncalibrated,
    Exact,
    AdditiveFrequency,
    RelativeCardinality,
    RankQuantile,
    RelativeQuantile,
    TopK,
}

impl AccuracyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uncalibrated => "uncalibrated",
            Self::Exact => "exact",
            Self::AdditiveFrequency => "additive_frequency",
            Self::RelativeCardinality => "relative_cardinality",
            Self::RankQuantile => "rank_quantile",
            Self::RelativeQuantile => "relative_quantile",
            Self::TopK => "top_k",
        }
    }
}

/// Accuracy metadata shared by planning and serving.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AccuracyProfile {
    pub epsilon: f64,
    /// Failure probability; None means the planner cannot establish it.
    pub delta: Option<f64>,
    pub kind: AccuracyKind,
}

impl AccuracyProfile {
    /// Exact (ε = δ = 0).
    pub fn exact() -> Self {
        Self {
            epsilon: 0.0,
            delta: Some(0.0),
            kind: AccuracyKind::Exact,
        }
    }

    /// One-line summary for dashboards and logs.
    pub fn summary(&self) -> String {
        format!(
            "accuracy: ε={}, δ={}, kind={}",
            self.epsilon,
            self.delta
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into()),
            self.kind.as_str()
        )
    }

    /// Project the four shared family contracts without reimplementing their
    /// error formulas. Other families keep their backend-specific adapters.
    pub fn from_sketch_params(params: &planner_types::post_asap::SketchParams) -> Option<Self> {
        use asap_aware_mapping::accuracy::DefaultAccuracyModel;
        use planner_types::post_asap::{SketchAlgorithm, SketchParams, SketchQuery};
        use planner_types::pre_asap::ColumnRef;
        let (algorithm, query, kind) = match params {
            SketchParams::Cms { .. } => (
                SketchAlgorithm::Cms,
                SketchQuery::PointCount {
                    key: ColumnRef::SampleValue,
                    value: None,
                },
                AccuracyKind::AdditiveFrequency,
            ),
            SketchParams::Hll { .. } => (
                SketchAlgorithm::Hll,
                SketchQuery::Cardinality,
                AccuracyKind::RelativeCardinality,
            ),
            SketchParams::Kll { .. } => (
                SketchAlgorithm::Kll,
                SketchQuery::Quantile { q: 0.5 },
                AccuracyKind::RankQuantile,
            ),
            SketchParams::DDSketch { .. } => (
                SketchAlgorithm::DDSketch,
                SketchQuery::Quantile { q: 0.5 },
                AccuracyKind::RelativeQuantile,
            ),
            _ => return None,
        };
        let guarantee = DefaultAccuracyModel::sketch_guarantee(&algorithm, params, &query)?;
        Some(Self {
            epsilon: guarantee.bound.evaluate()?,
            delta: guarantee.failure_probability.evaluate(),
            kind,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_contract_round_trips_with_stable_kind_name() {
        let profile = AccuracyProfile {
            epsilon: 0.01,
            delta: Some(0.001),
            kind: AccuracyKind::AdditiveFrequency,
        };
        let json = serde_json::to_string(&profile).unwrap();
        assert_eq!(
            serde_json::from_str::<AccuracyProfile>(&json).unwrap(),
            profile
        );
        assert!(json.contains("additive_frequency"));
    }
}
