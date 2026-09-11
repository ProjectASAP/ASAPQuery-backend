//! Content-addressed policy identity.
//!
//! `PolicyFingerprint` is the merged-sid-identity-chain replacement for
//! the controller-allocated `aggregation_id: u64`. Where `aggregation_id`
//! is a counter the control plane mints and ships in the streaming-config
//! YAML, `PolicyFingerprint` is derived deterministically from the
//! `AggregationConfig`'s content — so two control planes producing the
//! same policy independently produce the same fingerprint, and the data
//! plane can index without a separate id allocation.
//!
//! ## Identity contract
//!
//! `PolicyFingerprint = h(metric, agg_type, sub_type, parameters,
//! grouping_labels, aggregated_labels, rollup_labels, window_size,
//! slide_interval, window_type, pane_origin_ms, spatial_filter_normalized)`
//!
//! The hash includes **every** field of `AggregationConfig` that
//! determines what the policy does — sketch / exact-agg shape,
//! group-by + rollup layout, window cadence, spatial filter. Two
//! configs that compare equal on these dimensions produce the same
//! fingerprint; two that differ produce different fingerprints.
//!
//! Fields *excluded* from the fingerprint:
//! - `aggregation_id` itself (the thing we're replacing — it's a
//!   downstream label, not part of identity).
//! - `original_yaml` (incidental serialization artifact).
//! - `num_aggregates_to_retain` (retention policy, not aggregation
//!   semantics — two policies with the same shape but different
//!   retention are *the same policy* for ingest/query routing
//!   purposes; retention is a separate concern).
//! - `table_name` / `value_column` (SQL-mode wire shape; folded into
//!   `metric` upstream for time-series mode).
//!
//! ## Hash function
//!
//! `xxh64` keyed at 0, matching the existing `compute_agg_config_id`
//! helper this replaces. 64-bit gives ~4B-policy birthday bound
//! (collision probability ~10⁻¹¹ at 100K live policies); ample for
//! foreseeable workloads. Bump to sha256 if the control plane ever
//! manages >10⁶ live policies and we want deterministic uniqueness.
//!
//! The fingerprint is **stable across hosts and versions**: the byte
//! layout this module produces is the contract. Don't reorder fields,
//! don't change separator bytes — any such change invalidates every
//! deployed fingerprint and forces a cold-start rebuild.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use xxhash_rust::xxh64::xxh64;

use crate::aggregation_config::AggregationConfig;

/// Stable, content-addressed handle for an `AggregationConfig`.
///
/// Wrap a `u64` so callers can't accidentally swap a `PolicyFingerprint`
/// with an `aggregation_id` — they're both u64-shaped but they index
/// different things (content-addressed vs. controller-allocated).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PolicyFingerprint(pub u64);

impl PolicyFingerprint {
    /// Sentinel "unset / legacy" fingerprint produced by
    /// `PolicyFingerprint::default()`. PR 5 retired the
    /// `aggregation_id` field on `AggregationConfig`, so there's no
    /// legacy id to fall back to anymore — production sinks now treat
    /// `UNSET` as a control-plane bug worth surfacing. Callers should
    /// always populate `policy_fp` via [`Self::from_config`] at
    /// construction.
    pub const UNSET: PolicyFingerprint = PolicyFingerprint(0);

    /// True when this fingerprint is the [`Self::UNSET`] sentinel.
    pub fn is_unset(self) -> bool {
        self.0 == 0
    }
}

impl PolicyFingerprint {
    /// Compute the fingerprint of an [`AggregationConfig`].
    ///
    /// Hash inputs are concatenated with `\0` byte separators and
    /// canonicalized so that map/iteration order can't affect the
    /// outcome. Parameter values are rendered via `serde_json::to_string`
    /// for nested-shape determinism (matches the existing
    /// `parameters_canonical` form used in `AggKind::ExactAgg`).
    pub fn from_config(cfg: &AggregationConfig) -> Self {
        let mut buf: Vec<u8> = Vec::with_capacity(512);

        // 1. metric name
        buf.extend_from_slice(cfg.metric.as_bytes());
        buf.push(0);

        // 2. aggregation_type (Serialize impl is the stable form)
        buf.extend_from_slice(
            serde_json::to_string(&cfg.aggregation_type)
                .unwrap_or_default()
                .as_bytes(),
        );
        buf.push(0);

        // 3. aggregation_sub_type
        buf.extend_from_slice(cfg.aggregation_sub_type.as_bytes());
        buf.push(0);

        // 4. parameters — canonicalized (sorted keys, JSON-rendered values)
        let sorted: BTreeMap<&String, &serde_json::Value> = cfg.parameters.iter().collect();
        for (k, v) in sorted {
            buf.extend_from_slice(k.as_bytes());
            buf.push(b'=');
            buf.extend_from_slice(serde_json::to_string(v).unwrap_or_default().as_bytes());
            buf.push(b';');
        }
        buf.push(0);

        if let Some(partitioning) = cfg.partitioning {
            buf.extend_from_slice(format!("partition:{partitioning:?}\0").as_bytes());
        }

        // 5. grouping_labels (already sorted at construction per
        //    KeyByLabelNames invariant; encode as `,`-joined list)
        for l in &cfg.grouping_labels.labels {
            buf.extend_from_slice(l.as_bytes());
            buf.push(b',');
        }
        buf.push(0);

        // 6. aggregated_labels
        for l in &cfg.aggregated_labels.labels {
            buf.extend_from_slice(l.as_bytes());
            buf.push(b',');
        }
        buf.push(0);

        // 7. rollup_labels
        for l in &cfg.rollup_labels.labels {
            buf.extend_from_slice(l.as_bytes());
            buf.push(b',');
        }
        buf.push(0);

        // 8. window_size + slide_interval + window_type (cadence)
        buf.extend_from_slice(&cfg.window_size.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&cfg.slide_interval.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(
            serde_json::to_string(&cfg.window_type)
                .unwrap_or_default()
                .as_bytes(),
        );
        buf.push(0);

        // Physical representation is part of state identity. A full overlapping
        // window and a mergeable pane layout may share semantic descriptors but
        // never share payload instances or lifecycle accounting.
        buf.extend_from_slice(
            serde_json::to_string(&cfg.window_layout)
                .unwrap_or_default()
                .as_bytes(),
        );
        buf.push(0);

        // 9. pane origin. Presence is explicit so a legacy definition with
        // unknown phase cannot alias an epoch-aligned definition.
        match cfg.pane_origin_ms {
            Some(origin) => {
                buf.push(1);
                buf.extend_from_slice(&origin.to_le_bytes());
            }
            None => buf.push(0),
        }
        buf.push(0);

        // 10. spatial_filter_normalized — canonicalized predicate
        buf.extend_from_slice(cfg.spatial_filter_normalized.as_bytes());
        if let Some(table) = &cfg.table_name {
            buf.extend_from_slice(b"\0sql-source-v1\0");
            buf.extend_from_slice(table.as_bytes());
            buf.push(0);
            if let Some(column) = &cfg.value_column {
                buf.extend_from_slice(column.as_bytes());
            }
        }
        if let Some(population) = &cfg.table_population {
            let canonical = population.canonical();
            if !canonical.is_empty() {
                buf.push(0);
                buf.extend_from_slice(canonical.as_bytes());
            }
        }
        if let Some(column) = &cfg.table_timestamp_column {
            buf.extend_from_slice(b"\0timestamp-ms\0");
            buf.extend_from_slice(column.as_bytes());
        }

        Self(xxh64(&buf, 0))
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
    use crate::AggregationType;
    use crate::KeyByLabelNames;
    use std::collections::HashMap;

    fn cfg(
        metric: &str,
        agg_type: AggregationType,
        params: HashMap<String, serde_json::Value>,
        group_by: Vec<&str>,
        window_size: u64,
        spatial_filter: &str,
    ) -> AggregationConfig {
        AggregationConfig::new(
            agg_type,
            String::new(),
            params,
            KeyByLabelNames::new(group_by.into_iter().map(|s| s.to_string()).collect()),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            window_size,
            window_size,
            WindowKind::Tumbling,
            spatial_filter.to_string(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    #[test]
    fn same_config_yields_same_fingerprint() {
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec!["zone"],
            60,
            "",
        );
        let b = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec!["zone"],
            60,
            "",
        );
        assert_eq!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    #[test]
    fn physical_window_layout_is_part_of_state_identity() {
        let panes = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec!["zone"],
            60,
            "",
        );
        let mut full = panes.clone();
        full.window_layout = crate::WindowMaterializationLayout::FullWindow;
        assert_ne!(
            PolicyFingerprint::from_config(&panes),
            PolicyFingerprint::from_config(&full)
        );
    }

    #[test]
    fn different_metric_yields_different_fingerprint() {
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            "",
        );
        let b = cfg(
            "cpu_pct",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            "",
        );
        assert_ne!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    #[test]
    fn different_window_yields_different_fingerprint() {
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            "",
        );
        let b = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            300,
            "",
        );
        assert_ne!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    #[test]
    fn different_spatial_filter_yields_different_fingerprint() {
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            "",
        );
        let b = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            r#"status="200""#,
        );
        assert_ne!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    #[test]
    fn different_group_by_yields_different_fingerprint() {
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec!["zone"],
            60,
            "",
        );
        let b = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec!["zone", "service"],
            60,
            "",
        );
        assert_ne!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    /// Pre-PR-5 the `aggregation_id` field on `AggregationConfig` was
    /// excluded from the fingerprint hash. PR 5 deletes the field
    /// entirely — identity *is* the fingerprint — so this is now
    /// vacuously true. Kept as a doc-comment anchor; no runtime test
    /// is needed.
    #[test]
    fn policy_fp_u64_accessor_equals_fingerprint_u64() {
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            "",
        );
        assert_eq!(
            a.policy_fp_u64(),
            PolicyFingerprint::from_config(&a).as_u64(),
        );
    }

    #[test]
    fn num_aggregates_to_retain_does_not_affect_fingerprint() {
        let mut a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            "",
        );
        let mut b = a.clone();
        a.num_aggregates_to_retain = Some(100);
        b.num_aggregates_to_retain = Some(500);
        assert_eq!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b),
            "retention is a separate concern from policy identity"
        );
    }

    #[test]
    fn parameter_map_order_does_not_affect_fingerprint() {
        // HashMap iteration order is non-deterministic; the fingerprint
        // must be order-independent.
        let mut p1 = HashMap::new();
        p1.insert("a".into(), serde_json::json!(1));
        p1.insert("b".into(), serde_json::json!(2));
        let mut p2 = HashMap::new();
        p2.insert("b".into(), serde_json::json!(2));
        p2.insert("a".into(), serde_json::json!(1));
        let a = cfg("http_lat", AggregationType::Sum, p1, vec![], 60, "");
        let b = cfg("http_lat", AggregationType::Sum, p2, vec![], 60, "");
        assert_eq!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    #[test]
    fn spatial_filter_canonicalization_drives_fingerprint() {
        // Two filters that differ only in matcher ordering produce the
        // SAME normalized form, hence the SAME fingerprint. The
        // canonicalization step in `AggregationConfig::new` (via
        // `normalize_spatial_filter`) sorts matchers by key.
        let a = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            r#"status="200",zone="us""#,
        );
        let b = cfg(
            "http_lat",
            AggregationType::Sum,
            HashMap::new(),
            vec![],
            60,
            r#"zone="us",status="200""#,
        );
        assert_eq!(
            PolicyFingerprint::from_config(&a),
            PolicyFingerprint::from_config(&b)
        );
    }

    #[test]
    fn display_format_is_hex_with_prefix() {
        let fp = PolicyFingerprint(0xdeadbeef);
        assert_eq!(format!("{}", fp), "policy_fp:00000000deadbeef");
    }
}
