//! `RoutingIndex` — a metric-bucketed structural index over a
//! [`PolicyRegistry`]. It is sourced from the content-addressed view over a
//! `StreamingConfig`'s `AggregationConfig`s, so it represents planned policy
//! rather than a reconstruction from ingest side effects.
//!
//! **Tier 1** (exact `PolicyFingerprint` → config) is [`PolicyRegistry::get`]
//! itself — already O(1), nothing to add here.
//!
//! **Tier 2** (structural match: "every policy registered for this metric")
//! is what this type adds. Before this existed,
//! `control_plane::asap_tier_analysis::find_matching_policies` scanned
//! *every* policy in the registry for every candidate, checking each one's
//! metric name first — i.e. it paid for every OTHER metric's policies on
//! every lookup. `RoutingIndex` buckets by metric once, at construction
//! time, so a lookup only ever touches the policies that could possibly
//! match.
//!
//! ## Lifecycle
//!
//! Built fresh from a `PolicyRegistry` snapshot — construction is `O(N)`
//! over the registry, same order as `PolicyRegistry::from_configs` itself.
//! Callers that build one per query (mirroring today's
//! `streaming_snap.policy_registry()` call) still get the Tier-2 win for
//! any query with more than one candidate sharing the same snapshot
//! (composed PromQL shapes routinely do). Building it once per
//! `StreamingConfig` hot-reload swap instead of once per query — the same
//! "cheap, but call at swap time not per query, if it shows up in
//! profiles" note `StreamingConfig::policy_registry`'s own doc comment
//! already flags — is a further, larger change (it means threading a
//! cached derived value through `HotReloadStreamingConfig`'s swap path)
//! and is not done by this type on its own.

use std::collections::{BTreeSet, HashMap};

use crate::aggregation_config::AggregationConfig;
use crate::policy_fingerprint::PolicyFingerprint;
use crate::policy_registry::PolicyRegistry;

/// See module docs.
#[derive(Debug, Clone, Default)]
pub struct RoutingIndex {
    registry: PolicyRegistry,
    by_metric: HashMap<String, Vec<PolicyFingerprint>>,
}

impl RoutingIndex {
    /// Build from a `PolicyRegistry` snapshot. Takes ownership rather than
    /// borrowing — callers that still need their own `PolicyRegistry`
    /// handle after this should `.clone()` it first (cheap-ish, but real;
    /// most callers don't need the raw registry once they have the index,
    /// since [`Self::get`] delegates straight through).
    pub fn build(registry: PolicyRegistry) -> Self {
        let mut by_metric: HashMap<String, Vec<PolicyFingerprint>> = HashMap::new();
        for (fp, cfg) in registry.iter() {
            by_metric.entry(cfg.metric.clone()).or_default().push(*fp);
        }
        Self {
            registry,
            by_metric,
        }
    }

    /// Tier 1 — exact fingerprint lookup. Delegates to the underlying
    /// registry; see [`PolicyRegistry::get`].
    pub fn get(&self, fp: PolicyFingerprint) -> Option<&AggregationConfig> {
        self.registry.get(fp)
    }

    /// Tier 2 — every policy fingerprint registered for `metric`, in
    /// registration order. Empty slice (not an `Option`/error) when
    /// nothing is registered for this metric — callers already treat "no
    /// candidates" as a normal, expected outcome (capability miss →
    /// archive fallback), not a failure to report.
    pub fn candidates_for_metric(&self, metric: &str) -> &[PolicyFingerprint] {
        self.by_metric.get(metric).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Live policy count (same as the underlying registry's).
    pub fn len(&self) -> usize {
        self.registry.len()
    }

    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    /// All fingerprints currently registered. Delegates to the underlying
    /// registry — see [`PolicyRegistry::fingerprints`].
    pub fn fingerprints(&self) -> impl Iterator<Item = PolicyFingerprint> + '_ {
        self.registry.fingerprints()
    }

    /// Resolve an ingest-side sketch shape to exactly one configured policy.
    /// Ambiguous or absent matches fail closed.
    pub fn find_policy_by_content(
        &self,
        metric: &str,
        group_by_keys: &BTreeSet<String>,
        agg_type: crate::AggregationType,
        expected_params: &HashMap<String, serde_json::Value>,
    ) -> Option<PolicyFingerprint> {
        let mut hit = None;
        for fp in self.candidates_for_metric(metric) {
            let cfg = self.get(*fp)?;
            let policy_keys: BTreeSet<_> = cfg.grouping_labels.iter().cloned().collect();
            if cfg.aggregation_type != agg_type
                || &policy_keys != group_by_keys
                || !cfg.spatial_filter_normalized.is_empty()
                || !expected_params.iter().all(|(key, value)| {
                    cfg.parameters.get(key).or_else(|| match key.as_str() {
                        // The typed physical-plan compiler names this field
                        // after SummaryParams, while legacy collector YAML
                        // uses the equivalent runtime-facing name.
                        "relative_accuracy" => cfg.parameters.get("alpha"),
                        "alpha" => cfg.parameters.get("relative_accuracy"),
                        _ => None,
                    }) == Some(value)
                })
            {
                continue;
            }
            if hit.is_some() {
                return None;
            }
            hit = Some(*fp);
        }
        hit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::WindowKind;
    use crate::AggregationType;
    use crate::KeyByLabelNames;
    use std::collections::HashMap as StdHashMap;

    fn cfg(metric: &str) -> AggregationConfig {
        AggregationConfig::new(
            AggregationType::Sum,
            String::new(),
            StdHashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    #[test]
    fn buckets_by_metric() {
        let a1 = cfg("http_lat");
        let a2 = cfg("cpu_pct");
        let fp_a1 = PolicyFingerprint::from_config(&a1);
        let fp_a2 = PolicyFingerprint::from_config(&a2);
        let idx = RoutingIndex::build(PolicyRegistry::from_configs(vec![a1, a2]));

        assert_eq!(idx.candidates_for_metric("http_lat"), &[fp_a1]);
        assert_eq!(idx.candidates_for_metric("cpu_pct"), &[fp_a2]);
    }

    #[test]
    fn multiple_policies_for_the_same_metric_all_bucket_together() {
        // Same metric, distinct group-by shapes -> distinct fingerprints,
        // same bucket.
        let a = AggregationConfig::new(
            AggregationType::Sum,
            String::new(),
            StdHashMap::new(),
            KeyByLabelNames::new(vec!["zone".to_string()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "http_lat".to_string(),
            None,
            None,
            None,
        );
        let b = cfg("http_lat");
        assert_ne!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
        let idx = RoutingIndex::build(PolicyRegistry::from_configs(vec![a, b]));
        assert_eq!(idx.candidates_for_metric("http_lat").len(), 2);
    }

    #[test]
    fn unknown_metric_returns_empty_slice_not_missing() {
        let idx = RoutingIndex::build(PolicyRegistry::from_configs(vec![cfg("http_lat")]));
        assert!(idx.candidates_for_metric("no_such_metric").is_empty());
    }

    #[test]
    fn get_delegates_to_underlying_registry() {
        let a = cfg("http_lat");
        let fp = PolicyFingerprint::from_config(&a);
        let idx = RoutingIndex::build(PolicyRegistry::from_configs(vec![a]));
        assert_eq!(
            idx.get(fp).map(|c| c.metric.clone()),
            Some("http_lat".to_string())
        );
        assert!(idx
            .get(PolicyFingerprint::from_config(&cfg("nope")))
            .is_none());
    }

    #[test]
    fn len_and_is_empty_match_registry() {
        let idx =
            RoutingIndex::build(PolicyRegistry::from_configs(Vec::<AggregationConfig>::new()));
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);

        let idx = RoutingIndex::build(PolicyRegistry::from_configs(vec![cfg("m")]));
        assert!(!idx.is_empty());
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn ddsketch_alpha_and_relative_accuracy_are_wire_compatible() {
        let mut parameters = StdHashMap::new();
        parameters.insert("alpha".to_string(), serde_json::json!(0.01));
        let config = AggregationConfig::new(
            AggregationType::DDSketch,
            String::new(),
            parameters,
            KeyByLabelNames::new(vec!["service".to_string()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            5,
            5,
            WindowKind::Tumbling,
            String::new(),
            "latency".to_string(),
            None,
            None,
            None,
        );
        let fingerprint = PolicyFingerprint::from_config(&config);
        let index = RoutingIndex::build(PolicyRegistry::from_configs(vec![config]));
        let expected =
            StdHashMap::from([("relative_accuracy".to_string(), serde_json::json!(0.01))]);

        assert_eq!(
            index.find_policy_by_content(
                "latency",
                &BTreeSet::from(["service".to_string()]),
                AggregationType::DDSketch,
                &expected,
            ),
            Some(fingerprint)
        );
    }
}
