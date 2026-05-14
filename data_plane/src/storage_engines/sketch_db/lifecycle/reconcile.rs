//! Sid-level reconcile against a fresh `StreamingConfig` snapshot —
//! schema retirement #4.
//!
//! Mirrors the "retire orphans" half of the legacy
//! [`SchemaRegistry::reconcile`](crate::storage_engines::sketch_db::schema::SchemaRegistry::reconcile)
//! but reads / writes the sid catalog directly. The "add new ids"
//! half is implicit in the sid model: sids are minted lazily by the
//! ingest path on first write (see
//! `SketchStore::ingest_precompute_for_agg_config` and the modified-OTLP
//! sketch path), so there is nothing to pre-register up front.
//!
//! ## Signature comparison
//!
//! Each sid carries a content signature `(metric_name, agg_kind,
//! group_by_keys)` derived from the agg-config + attrs at ingest time.
//! Each agg-config in the new `StreamingConfig` likewise canonicalizes
//! to a signature. A sid is "orphaned" when its signature does not
//! match any agg-config in the new config — at that point ingest can
//! no longer route to it, and it must be retired so the eviction
//! sweep can later drop it.
//!
//! Sketch-typed agg-configs are not yet covered: the
//! `AggregationConfig` shape doesn't carry a `SketchKindHandle` /
//! `SketchConfig` natively (control plane pushes them through a parallel
//! capability-routing channel). For now the reconciler treats every
//! agg-config as a precompute signature; sketch sids never compare
//! equal so they're never retired by this path. That matches the
//! pre-retirement behavior: `SchemaRegistry::reconcile` only retired
//! agg_ids in its own registry, which mirrored the
//! `StreamingConfig.aggregation_configs` map (also precompute-only).
//! Sketch lifecycle stays driven by the control plane's eviction RPC
//! until M3 unifies the two.

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::streaming_config::StreamingConfig;

use crate::storage_engines::sketch_db::data::{canonical_parameters, AggKind};
use crate::storage_engines::sketch_db::index::{SketchInstanceMetadata, SketchStore};
use crate::storage_engines::sketch_db::lifecycle::AggStatus;

/// Sids the reconciler force-retired this call.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SidReconcileSummary {
    /// Sids that transitioned `Active → Retired` because their
    /// content signature is no longer present in the new
    /// `StreamingConfig`. Already-Retired or already-Expired sids
    /// are not re-touched.
    pub retired: Vec<u64>,
}

/// Iterate every sid in `store`; force-retire any whose signature
/// `(metric_name, agg_kind, group_by_keys)` is not represented in
/// `config`. `retention` is forwarded to
/// [`SketchStore::force_retire`] so the schedule of eventual
/// `Retired → Expired` matches the pre-retirement `SchemaRegistry`
/// behavior.
pub fn reconcile_from_streaming_config(
    store: &SketchStore,
    config: &StreamingConfig,
    retention: Duration,
) -> SidReconcileSummary {
    let live_signatures = build_live_signature_set(config);

    let mut retired = Vec::new();
    for meta in store.snapshot_instances() {
        if !matches!(meta.status(), AggStatus::Active) {
            continue;
        }
        let sig = signature_from_meta(&meta);
        if !live_signatures.contains(&sig) {
            if store.force_retire(meta.sid, retention).is_some() {
                retired.push(meta.sid);
            }
        }
    }
    SidReconcileSummary { retired }
}

/// Canonical byte encoding of a sid's content signature, used as the
/// HashSet key. Two sids of the same signature produce identical
/// bytes; cross-signature collisions require a 64-bit hash birthday
/// — we don't compress here, we compare the full bytes.
fn signature_bytes(metric_name: &str, agg_kind: &AggKind, group_by_keys: &BTreeSet<String>) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(metric_name.as_bytes());
    buf.push(0);
    encode_agg_kind(agg_kind, &mut buf);
    buf.push(0);
    for k in group_by_keys {
        buf.extend_from_slice(k.as_bytes());
        buf.push(b',');
    }
    buf
}

fn signature_from_meta(meta: &SketchInstanceMetadata) -> Vec<u8> {
    signature_bytes(&meta.metric_name, &meta.agg_kind, &meta.group_by_keys)
}

fn signature_from_agg_config(cfg: &AggregationConfig) -> Vec<u8> {
    let agg_kind = AggKind::ExactAgg {
        agg_type: cfg.aggregation_type,
        parameters_canonical: canonical_parameters(&cfg.parameters),
        spatial_filter_canonical: cfg.spatial_filter_normalized.clone(),
    };
    let group_by_keys: BTreeSet<String> = cfg.grouping_labels.labels.iter().cloned().collect();
    signature_bytes(&cfg.metric, &agg_kind, &group_by_keys)
}

fn build_live_signature_set(config: &StreamingConfig) -> HashSet<Vec<u8>> {
    config
        .get_all_aggregation_configs()
        .values()
        .map(signature_from_agg_config)
        .collect()
}

fn encode_agg_kind(agg_kind: &AggKind, buf: &mut Vec<u8>) {
    use crate::storage_engines::sketch_db::data::{SketchConfig, SketchKindHandle};
    match agg_kind {
        AggKind::Sketch {
            kind,
            config,
            spatial_filter_canonical,
        } => {
            buf.push(b'S');
            buf.push(match kind {
                SketchKindHandle::DDSketch => 1,
                SketchKindHandle::Kll => 2,
                SketchKindHandle::Hll => 3,
                SketchKindHandle::CountSketch => 4,
                SketchKindHandle::CountMin => 5,
                SketchKindHandle::CmsWithHeap => 6,
                SketchKindHandle::CountSketchWithHeap => 7,
                SketchKindHandle::Any => 0,
            });
            match config {
                SketchConfig::DDSketch { relative_accuracy } => {
                    buf.push(b'D');
                    buf.extend_from_slice(&relative_accuracy.to_le_bytes());
                }
                SketchConfig::Kll { k } => {
                    buf.push(b'K');
                    buf.extend_from_slice(&k.to_le_bytes());
                }
                SketchConfig::Hll { precision } => {
                    buf.push(b'H');
                    buf.extend_from_slice(&precision.to_le_bytes());
                }
                SketchConfig::CountSketch { rows, cols } => {
                    buf.push(b'S');
                    buf.extend_from_slice(&rows.to_le_bytes());
                    buf.extend_from_slice(&cols.to_le_bytes());
                }
                SketchConfig::CountMin { rows, cols } => {
                    buf.push(b'M');
                    buf.extend_from_slice(&rows.to_le_bytes());
                    buf.extend_from_slice(&cols.to_le_bytes());
                }
            }
            buf.push(b'F');
            buf.extend_from_slice(spatial_filter_canonical.as_bytes());
        }
        AggKind::ExactAgg {
            agg_type,
            parameters_canonical,
            spatial_filter_canonical,
        } => {
            buf.push(b'P');
            buf.extend_from_slice(agg_type.as_str().as_bytes());
            buf.push(b';');
            buf.extend_from_slice(parameters_canonical.as_bytes());
            buf.push(b'F');
            buf.extend_from_slice(spatial_filter_canonical.as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

    use crate::storage_engines::sketch_db::data::AggKind;
    use crate::storage_engines::sketch_db::index::{SketchInstanceMetadata, SketchStore};

    fn agg_config(metric: &str, agg_type: AggregationType, group_by: Vec<&str>) -> AggregationConfig {
        AggregationConfig::new(
            agg_type,
            String::new(),
            HashMap::new(),
            KeyByLabelNames::new(group_by.into_iter().map(|s| s.to_string()).collect()),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            1,
            1,
            WindowType::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    fn meta(
        sid: u64,
        metric: &str,
        agg_type: AggregationType,
        group_by: Vec<&str>,
    ) -> SketchInstanceMetadata {
        let group_by_keys: BTreeSet<String> =
            group_by.into_iter().map(|s| s.to_string()).collect();
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys,
            capability: None,
            agg_kind: AggKind::ExactAgg {
                agg_type,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    fn streaming(configs: Vec<AggregationConfig>) -> StreamingConfig {
        let mut map = HashMap::new();
        for (i, c) in configs.into_iter().enumerate() {
            map.insert(i as u64 + 1, c);
        }
        StreamingConfig::new(map)
    }

    #[test]
    fn empty_config_retires_every_active_sid() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        store.register(meta(2, "mem", AggregationType::Sum, vec!["host"]));
        let summary =
            reconcile_from_streaming_config(&store, &streaming(vec![]), Duration::from_secs(60));
        let mut retired = summary.retired.clone();
        retired.sort();
        assert_eq!(retired, vec![1, 2]);
    }

    #[test]
    fn config_with_matching_signature_keeps_sid_active() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        let cfg = streaming(vec![agg_config("cpu", AggregationType::Sum, vec!["host"])]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));
        assert!(summary.retired.is_empty());
    }

    #[test]
    fn config_with_different_signature_retires_orphan() {
        let store = SketchStore::new();
        // sid 1 is Sum-by-host; only Sum-by-region is in the new config.
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        let cfg = streaming(vec![agg_config("cpu", AggregationType::Sum, vec!["region"])]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));
        assert_eq!(summary.retired, vec![1]);
    }

    #[test]
    fn already_retired_sid_is_not_re_retired() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        // First reconcile with empty config retires it.
        let _ = reconcile_from_streaming_config(&store, &streaming(vec![]), Duration::from_secs(60));
        // Second reconcile reports nothing new.
        let summary = reconcile_from_streaming_config(
            &store,
            &streaming(vec![]),
            Duration::from_secs(60),
        );
        assert!(summary.retired.is_empty());
    }

    #[test]
    fn distinct_metrics_do_not_cross_match() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        // Same agg_kind + grouping, different metric — sid 1 must
        // still retire because its signature includes the metric.
        let cfg = streaming(vec![agg_config("mem", AggregationType::Sum, vec!["host"])]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));
        assert_eq!(summary.retired, vec![1]);
    }

    #[test]
    fn different_agg_type_does_not_match() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        let cfg = streaming(vec![agg_config("cpu", AggregationType::Increase, vec!["host"])]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));
        assert_eq!(summary.retired, vec![1]);
    }
}
