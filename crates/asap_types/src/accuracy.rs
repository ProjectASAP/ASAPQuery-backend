//! Shared wire contract for theoretical accuracy metadata.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccuracyKind {
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
            Self::Exact => "exact",
            Self::AdditiveFrequency => "additive_frequency",
            Self::RelativeCardinality => "relative_cardinality",
            Self::RankQuantile => "rank_quantile",
            Self::RelativeQuantile => "relative_quantile",
            Self::TopK => "top_k",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AccuracyProfile {
    pub epsilon: f64,
    pub delta: f64,
    pub kind: AccuracyKind,
}

impl AccuracyProfile {
    pub fn exact() -> Self {
        Self {
            epsilon: 0.0,
            delta: 0.0,
            kind: AccuracyKind::Exact,
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "accuracy: ε={}, δ={}, kind={}",
            self.epsilon,
            self.delta,
            self.kind.as_str()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_contract_round_trips_with_stable_kind_name() {
        let profile = AccuracyProfile {
            epsilon: 0.01,
            delta: 0.001,
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
