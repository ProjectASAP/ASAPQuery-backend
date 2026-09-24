//! Legacy routing wrapper for a deployed stored output.
//!
//! An explicit `PrecomputeMaterialization::stored_output_id` takes precedence.
//! Otherwise the compiler allocates a deterministic default from the existing
//! policy fields (including pane layout and cadence). This identifier is not
//! semantic identity: `SummaryDefinitionId` hashes the versioned semantic
//! definition, and several deployed outputs may share that definition.
//! Catalog installation checks that an output is never assigned conflicting
//! computation or format contracts.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use xxhash_rust::xxh64::xxh64;

use crate::aggregation_config::PrecomputeMaterialization;

/// Routing handle for one deployed stored output. See the module contract.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PolicyFingerprint(pub u64);

impl PolicyFingerprint {
    /// Unset sentinel returned by `Default`. Sinks cannot resolve an unset policy;
    /// construct valid fingerprints with [`Self::from_config`].
    pub const UNSET: PolicyFingerprint = PolicyFingerprint(0);

    /// True when this fingerprint is the [`Self::UNSET`] sentinel.
    pub fn is_unset(self) -> bool {
        self.0 == 0
    }
}

impl PolicyFingerprint {
    /// Compute the fingerprint of an [`PrecomputeMaterialization`].
    ///
    /// Hash inputs are concatenated with `\0` byte separators and
    /// canonicalized so that map/iteration order can't affect the
    /// outcome. Parameter values are rendered via `serde_json::to_string`
    /// for nested-shape determinism (matches the existing
    /// `parameters_canonical` form used in `AggKind::ExactAgg`).
    pub fn from_config(cfg: &PrecomputeMaterialization) -> Self {
        if let Some(output) = cfg.stored_output_id {
            return output.fingerprint();
        }
        let mut buf: Vec<u8> = Vec::with_capacity(512);

        if !cfg.population_key_encoding.is_legacy() {
            // UTF-8 raw metrics cannot alias this nonlegacy domain prefix.
            buf.extend_from_slice(b"\xffpopulation-key-canonical-labels-v1\0");
        }
        // 1. metric name
        if cfg.derived_input.is_some() {
            // Raw policies start with UTF-8 metric bytes; 0xff is impossible
            // there, so a metric cannot impersonate this source domain.
            buf.extend_from_slice(b"\xffderived-input-v1:");
            buf.extend_from_slice(
                &serde_json::to_vec(&cfg.source_identity()).expect("typed source identity"),
            );
        } else {
            buf.extend_from_slice(cfg.metric.as_bytes());
        }
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
        for l in &cfg.grouping_labels.names() {
            buf.extend_from_slice(l.as_bytes());
            buf.push(b',');
        }
        buf.push(0);

        if cfg.table_name.is_some() && !cfg.grouping_labels.is_empty() {
            buf.extend_from_slice(
                crate::grouping_projection::TABLE_GROUP_OBSERVATION_SEMANTICS.as_bytes(),
            );
            buf.push(0);
        }
        if !cfg.grouping_labels.is_legacy_labels() {
            buf.extend_from_slice(b"typed-grouping:");
            buf.extend_from_slice(
                serde_json::to_string(&cfg.grouping_labels)
                    .expect("group projection serializes")
                    .as_bytes(),
            );
            buf.push(0);
        }

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
            if let Some(column) = cfg.effective_value_projection().column() {
                buf.extend_from_slice(column.as_bytes());
            }
            if matches!(
                cfg.effective_value_projection(),
                crate::sds::ValueProjectionIdentity::Constant { .. }
            ) {
                buf.extend_from_slice(b"\0constant-projection-v1\0");
                buf.extend_from_slice(
                    serde_json::to_string(cfg.effective_value_projection())
                        .expect("finite validated projection serializes")
                        .as_bytes(),
                );
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
    ) -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
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
    fn population_encoding_preserves_legacy_wire_and_separates_identity() {
        use crate::grouping_projection::PopulationKeyEncoding;
        let legacy = cfg(
            "m",
            AggregationType::Sum,
            HashMap::new(),
            vec!["host"],
            60,
            "",
        );
        let wire = serde_json::to_value(&legacy).unwrap();
        assert!(wire.get("population_key_encoding").is_none());
        let decoded: PrecomputeMaterialization = serde_json::from_value(wire).unwrap();
        assert!(decoded.population_key_encoding.is_legacy());
        assert_eq!(legacy.policy_fingerprint(), decoded.policy_fingerprint());
        let mut canonical = legacy.clone();
        canonical.population_key_encoding = PopulationKeyEncoding::CanonicalLabelsV1;
        assert_ne!(legacy.policy_fingerprint(), canonical.policy_fingerprint());
        let wire = serde_json::to_value(&canonical).unwrap();
        assert_eq!(wire["population_key_encoding"], "canonical_labels_v1");
        let decoded: PrecomputeMaterialization = serde_json::from_value(wire).unwrap();
        assert_eq!(decoded.policy_fingerprint(), canonical.policy_fingerprint());
        use crate::traits::SerializableToSink;
        let mut sink = canonical.serialize_to_json();
        // Transport wrappers supply the three label projections separately.
        sink["groupingLabels"] = serde_json::to_value(&canonical.grouping_labels).unwrap();
        sink["aggregatedLabels"] =
            serde_json::to_value(&canonical.aggregated_labels.labels).unwrap();
        sink["rollupLabels"] = serde_json::to_value(&canonical.rollup_labels.labels).unwrap();
        let decoded = PrecomputeMaterialization::deserialize_from_json(&sink).unwrap();
        assert_eq!(
            decoded.population_key_encoding,
            canonical.population_key_encoding
        );

        assert!(serde_json::from_str::<PopulationKeyEncoding>("\"canonical_labels_v2\"").is_err());
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

    /// Pre-PR-5 the `aggregation_id` field on `PrecomputeMaterialization` was
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
        // canonicalization step in `PrecomputeMaterialization::new` (via
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
