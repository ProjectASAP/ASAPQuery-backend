//! Content-addressed policy registry.
//!
//! Derived view over a collection of `AggregationConfig`s that maps
//! [`PolicyFingerprint`] → [`AggregationConfig`]. This is the
//! merged-sid-identity-chain replacement for the controller-allocated
//! `aggregation_id`-keyed `HashMap` that `data_plane`'s `StreamingConfig`
//! carries (see `data_plane::storage_engines::types::streaming_config`'s
//! module doc for why that type lives there, not here).
//!
//! ## Dual-keyed transition
//!
//! PR 3 (where this lives): the registry exists alongside the
//! `aggregation_id`-keyed map. Callers can opt into either lookup.
//! Construction is `O(N)` over the source configs; computation is
//! pure (no I/O, no mutation of the source).
//!
//! Subsequent PRs migrate one set of callers at a time off
//! `aggregation_id` → `PolicyFingerprint`, until the final PR can
//! delete the legacy index.
//!
//! ## Identity invariants
//!
//! Two `AggregationConfig`s that produce the same `PolicyFingerprint`
//! ARE the same policy. The registry treats this as a *deduplication*
//! invariant — if two distinct entries in the source `materializations_by_policy_fingerprint`
//! map produce the same fingerprint, the later one wins (last-write
//! semantics). In practice the source should never contain duplicates;
//! if it does, that's a control-plane bug worth surfacing in telemetry
//! (see [`PolicyRegistry::from_configs_with_collisions`]).

use std::collections::HashMap;

use crate::aggregation_config::AggregationConfig;
use crate::policy_fingerprint::PolicyFingerprint;

/// Content-addressed lookup table for active aggregation policies.
#[derive(Debug, Clone, Default)]
pub struct PolicyRegistry {
    policies: HashMap<PolicyFingerprint, AggregationConfig>,
}

impl PolicyRegistry {
    /// Construct from a list of configs. Duplicates (same fingerprint)
    /// collapse to the last entry; use
    /// [`Self::from_configs_with_collisions`] when you want to detect
    /// them.
    pub fn from_configs<I>(configs: I) -> Self
    where
        I: IntoIterator<Item = AggregationConfig>,
    {
        let mut policies = HashMap::new();
        for cfg in configs {
            policies.insert(PolicyFingerprint::from_config(&cfg), cfg);
        }
        Self { policies }
    }

    /// Construct + report the count of duplicate fingerprints (entries
    /// where the source contained two configs producing the same
    /// fingerprint and the later one displaced the earlier). Zero in
    /// the well-formed case; non-zero is a control-plane bug worth
    /// surfacing.
    pub fn from_configs_with_collisions<I>(configs: I) -> (Self, usize)
    where
        I: IntoIterator<Item = AggregationConfig>,
    {
        let mut policies = HashMap::new();
        let mut collisions = 0usize;
        for cfg in configs {
            let fp = PolicyFingerprint::from_config(&cfg);
            if policies.insert(fp, cfg).is_some() {
                collisions += 1;
            }
        }
        (Self { policies }, collisions)
    }

    /// Look up the config for a fingerprint.
    pub fn get(&self, fp: PolicyFingerprint) -> Option<&AggregationConfig> {
        self.policies.get(&fp)
    }

    /// Iterate fingerprint → config pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&PolicyFingerprint, &AggregationConfig)> {
        self.policies.iter()
    }

    /// Live policy count.
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// All fingerprints currently registered. Useful for diffing two
    /// registries during a hot-reload swap.
    pub fn fingerprints(&self) -> impl Iterator<Item = PolicyFingerprint> + '_ {
        self.policies.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::WindowKind;
    use crate::AggregationType;
    use crate::KeyByLabelNames;
    use std::collections::HashMap as StdHashMap;

    fn cfg(_id: u64, metric: &str) -> AggregationConfig {
        // `_id` is unused after PR 5 — identity is derived from
        // content. Kept as a parameter so existing call sites in the
        // tests below don't churn.
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
    fn from_configs_round_trips_lookup() {
        let a = cfg(1, "http_lat");
        let b = cfg(2, "cpu_pct");
        let fp_a = PolicyFingerprint::from_config(&a);
        let fp_b = PolicyFingerprint::from_config(&b);
        let reg = PolicyRegistry::from_configs(vec![a.clone(), b.clone()]);
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(fp_a).unwrap().metric, "http_lat");
        assert_eq!(reg.get(fp_b).unwrap().metric, "cpu_pct");
    }

    #[test]
    fn identical_configs_collapse_to_one_entry() {
        // PR 5: identity IS the content. Two configs that are
        // byte-for-byte identical on the policy-relevant fields
        // collapse to ONE entry — there's no way to distinguish them
        // anymore.
        let a = cfg(1, "http_lat");
        let b = cfg(99, "http_lat");
        let (reg, collisions) = PolicyRegistry::from_configs_with_collisions(vec![a, b]);
        assert_eq!(reg.len(), 1);
        assert_eq!(collisions, 1);
    }

    #[test]
    fn distinct_policies_keep_distinct_entries() {
        let a = cfg(1, "http_lat");
        let b = cfg(1, "cpu_pct"); // different metric → distinct policies
        let (reg, collisions) = PolicyRegistry::from_configs_with_collisions(vec![a, b]);
        assert_eq!(reg.len(), 2);
        assert_eq!(collisions, 0);
    }
}
