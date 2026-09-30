//! Typed candidate diagnostics with the existing string wire representation.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(from = "String", into = "String")]
pub enum CandidateEvaluationStatus {
    #[default]
    Unspecified,
    CompilationFailed,
    AwaitingQuote,
    EvidenceMissing,
    ProviderRejected,
    EvidenceInvalid,
    Unselected,
    Selected,
    /// Preserve diagnostics from other producer versions during migration.
    Other(String),
}

impl CandidateEvaluationStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unspecified => "",
            Self::CompilationFailed => "bind_failed",
            Self::AwaitingQuote => "bound",
            Self::EvidenceMissing => "evidence_missing",
            Self::ProviderRejected => "rejected",
            Self::EvidenceInvalid => "evidence_invalid",
            Self::Unselected => "unselected",
            Self::Selected => "selected",
            Self::Other(value) => value,
        }
    }
}

impl From<String> for CandidateEvaluationStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "" => Self::Unspecified,
            "bind_failed" => Self::CompilationFailed,
            "bound" => Self::AwaitingQuote,
            "evidence_missing" => Self::EvidenceMissing,
            "rejected" => Self::ProviderRejected,
            "evidence_invalid" => Self::EvidenceInvalid,
            "unselected" => Self::Unselected,
            "selected" => Self::Selected,
            _ => Self::Other(value),
        }
    }
}

impl From<CandidateEvaluationStatus> for String {
    fn from(value: CandidateEvaluationStatus) -> Self {
        value.as_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Renaming diagnostic variants does not change known or future wire values.
    #[test]
    fn status_wire_values_round_trip() {
        for value in [
            "",
            "bind_failed",
            "bound",
            "evidence_missing",
            "rejected",
            "evidence_invalid",
            "unselected",
            "selected",
            "future_status",
        ] {
            let status: CandidateEvaluationStatus =
                serde_json::from_value(serde_json::json!(value)).unwrap();
            assert_eq!(status.as_str(), value);
            assert_eq!(serde_json::to_value(status).unwrap(), value);
        }
    }
}
