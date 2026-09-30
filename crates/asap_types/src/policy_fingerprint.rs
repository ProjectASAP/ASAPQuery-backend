//! Legacy routing wrapper for a deployed stored output.
//!
//! The value is `PrecomputeMaterialization::stored_output_id`, allocated by the
//! compiler from the deployment fields and the Planner producer's computation
//! identity. It is not semantic identity: `SummaryDefinitionId` hashes the
//! versioned semantic definition, and several deployed outputs may share it.

use serde::{Deserialize, Serialize};

use crate::aggregation_config::PrecomputeMaterialization;

/// Routing handle for one deployed stored output. See the module contract.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PolicyFingerprint(pub u64);

impl PolicyFingerprint {
    /// Unset sentinel returned by `Default`. Sinks cannot resolve an unset policy.
    pub const UNSET: PolicyFingerprint = PolicyFingerprint(0);

    /// True when this fingerprint is the [`Self::UNSET`] sentinel.
    pub fn is_unset(self) -> bool {
        self.0 == 0
    }

    /// The stored output id of `cfg`.
    pub fn from_config(cfg: &PrecomputeMaterialization) -> Self {
        cfg.stored_output_id.fingerprint()
    }

    /// The raw u64. Use sparingly — prefer comparing `PolicyFingerprint`
    /// values directly.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for PolicyFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hex form so logs distinguish a fingerprint from a decimal
        // counter id at a glance.
        write!(f, "policy_fp:{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::WindowKind;
    use crate::KeyByLabelNames;

    fn cfg(metric: &str, group_by: Vec<&str>, window_size: u64) -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
            metric,
            KeyByLabelNames::new(group_by.into_iter().map(|s| s.to_string()).collect()),
            window_size,
            window_size,
            WindowKind::Tumbling,
        )
    }

    fn allocated(
        mut config: PrecomputeMaterialization,
        computation: &str,
    ) -> PrecomputeMaterialization {
        config.allocate_stored_output_id(&computation);
        config
    }

    // The canonical wire keeps the legacy encoding implicit, and the
    // population key encoding separates allocated output identities.
    #[test]
    fn population_encoding_preserves_legacy_wire_and_separates_identity() {
        use crate::grouping_projection::PopulationKeyEncoding;
        let legacy = allocated(cfg("m", vec!["host"], 60), "sum");
        let wire = serde_json::to_value(&legacy).unwrap();
        assert!(wire.get("population_key_encoding").is_none());
        let decoded: PrecomputeMaterialization = serde_json::from_value(wire).unwrap();
        assert!(decoded.population_key_encoding.is_legacy());
        assert_eq!(legacy.policy_fingerprint(), decoded.policy_fingerprint());
        let mut canonical = cfg("m", vec!["host"], 60);
        canonical.population_key_encoding = PopulationKeyEncoding::CanonicalLabelsV1;
        let canonical = allocated(canonical, "sum");
        assert_ne!(legacy.policy_fingerprint(), canonical.policy_fingerprint());
        let wire = serde_json::to_value(&canonical).unwrap();
        assert_eq!(wire["population_key_encoding"], "canonical_labels_v1");
        let decoded: PrecomputeMaterialization = serde_json::from_value(wire).unwrap();
        assert_eq!(decoded.policy_fingerprint(), canonical.policy_fingerprint());
    }

    // Equal deployment fields and computation allocate the same output.
    #[test]
    fn same_deployment_and_computation_yield_same_output() {
        let a = allocated(cfg("http_lat", vec!["zone"], 60), "sum");
        let b = allocated(cfg("http_lat", vec!["zone"], 60), "sum");
        assert_eq!(a.policy_fingerprint(), b.policy_fingerprint());
        assert!(!a.policy_fingerprint().is_unset());
    }

    // Any deployment or computation difference allocates a distinct output.
    #[test]
    fn deployment_and_computation_differences_yield_distinct_outputs() {
        let base = allocated(cfg("m", vec!["zone"], 60), "sum");
        let mut panes = cfg("m", vec!["zone"], 60);
        panes.window_layout = crate::WindowMaterializationLayout::FullWindow;
        let mut phased = cfg("m", vec!["zone"], 60);
        phased.pane_origin_ms = Some(7_000);
        for other in [
            allocated(cfg("other", vec!["zone"], 60), "sum"),
            allocated(cfg("m", vec!["zone"], 30), "sum"),
            allocated(cfg("m", vec!["host"], 60), "sum"),
            allocated(cfg("m", vec!["zone"], 60), "count"),
            allocated(panes, "sum"),
            allocated(phased, "sum"),
        ] {
            assert_ne!(base.policy_fingerprint(), other.policy_fingerprint());
        }
    }

    // Retention is not part of an output's identity.
    #[test]
    fn retention_does_not_affect_output_identity() {
        let a = allocated(cfg("m", vec![], 60), "sum");
        let mut b = cfg("m", vec![], 60);
        b.num_aggregates_to_retain = Some(100);
        assert_eq!(
            a.policy_fingerprint(),
            allocated(b, "sum").policy_fingerprint()
        );
    }

    #[test]
    fn display_format_is_hex_with_prefix() {
        assert_eq!(
            format!("{}", PolicyFingerprint(0xdead_beef)),
            "policy_fp:00000000deadbeef"
        );
    }
}
