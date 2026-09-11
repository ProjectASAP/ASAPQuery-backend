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
//! `AggregationConfig` shape doesn't carry a `SketchAlgorithm` /
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
use std::sync::Arc;
use std::time::Duration;

use crate::storage_engines::types::StreamingConfig;
use asap_types::aggregation_config::AggregationConfig;

use crate::storage_engines::sketch_db::data::{canonical_parameters, AggKind};
use crate::storage_engines::sketch_db::index::SketchStore;
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
/// Ingest-path entry point: reconcile only when `config` is a config
/// the store has not yet reconciled against. The streaming config is a
/// lock-free `Arc<ArcSwap<StreamingConfig>>` whose `Arc` identity only
/// changes on a (rare) control-plane swap, so in steady state every
/// ingest batch hands us the *same* `Arc`. Gating on the `Arc` data
/// pointer collapses the per-batch reconcile to a single relaxed atomic
/// load in that common case, skipping the full catalog scan + any
/// signature derivation.
///
/// Pass the same `Arc<StreamingConfig>` the ingest batch snapshotted so
/// the pointer is stable; the snapshot is held alive for the call's
/// duration, so the pointer cannot be reused by a concurrently-dropped
/// config (no ABA hazard within a batch).
///
/// Returns `None` when the scan was skipped (config unchanged), or
/// `Some(summary)` with the retired sids when a reconcile actually ran.
pub fn reconcile_if_config_changed(
    store: &SketchStore,
    config: &Arc<StreamingConfig>,
    retention: Duration,
) -> Option<SidReconcileSummary> {
    let config_ptr = Arc::as_ptr(config) as usize;
    if !store.mark_reconciled_config(config_ptr) {
        return None;
    }
    Some(reconcile_from_streaming_config(store, config, retention))
}

pub fn reconcile_from_streaming_config(
    store: &SketchStore,
    config: &StreamingConfig,
    retention: Duration,
) -> SidReconcileSummary {
    let live_signatures = build_live_signature_set(config);

    // First pass: find orphaned Active sids without cloning the
    // catalog. `reconcile_from_streaming_config` runs on every ingest
    // batch, and the old `snapshot_instances()` here deep-cloned every
    // `SketchInstanceMetadata` (String + BTreeSet<String> + AggKind)
    // on each call — the dominant ingest-path CPU cost in profiling
    // (BTreeMap/String clone + malloc churn). We only need to read each
    // instance's signature under the read lock; collect just the cheap
    // `u64` sids to retire, then act after the lock drops.
    //
    // Reuse one scratch buffer across instances so signature
    // derivation doesn't allocate a fresh `Vec<u8>` per sid.
    let mut orphans: Vec<u64> = Vec::new();
    let mut scratch: Vec<u8> = Vec::new();
    store.for_each_instance(|sid, meta| {
        if !matches!(meta.status(), AggStatus::Active) {
            return;
        }
        // `live_signatures` is built exclusively from
        // `signature_from_agg_config`, which canonicalizes every
        // streaming-config `AggregationConfig` to an `AggKind::ExactAgg`
        // signature (`P`-prefixed). An `AggKind::Sketch` sid (OTLP
        // modified-sketch ingest path: KLL / HLL / DDSketch / CMS /
        // CountSketch) always produces an `S`-prefixed signature, so it
        // can NEVER be present in `live_signatures` — meaning this scan
        // would unconditionally orphan and retire EVERY sketch-backed sid
        // on the first config reconcile. That is exactly the opposite of
        // the documented intent (see this module's header: "sketch sids
        // never compare equal so they're never retired by this path").
        //
        // Retiring a sketch sid bars further ingest (the §6.3 write
        // barrier rejects writes to Retired/Expired sids), then eviction
        // drops its per-window state; the query path then classifies it
        // as `Ghost`, and the analyzer-driven dispatch short-circuits the
        // whole quantile / cardinality query to CapabilityMiss before it
        // can reach a freshly-minted Active sketch sid carrying live data.
        // Sketch-sid lifecycle is driven by the control plane's eviction
        // RPC, NOT by this precompute-shaped signature reconcile, so skip
        // them here.
        if matches!(meta.agg_kind, AggKind::Sketch { .. }) {
            return;
        }
        scratch.clear();
        signature_into(
            &meta.metric_name,
            &meta.agg_kind,
            &meta.group_by_keys,
            &mut scratch,
        );
        if !live_signatures.contains(scratch.as_slice()) {
            orphans.push(sid);
        }
    });

    // Second pass: retire the orphans (takes the write lock per sid).
    // Common case is zero orphans — steady-state ingest matches every
    // live signature — so this loop is usually empty.
    let mut retired = Vec::new();
    for sid in orphans {
        if store.force_retire(sid, retention).is_some() {
            retired.push(sid);
        }
    }
    SidReconcileSummary { retired }
}

/// Canonical byte encoding of a sid's content signature, used as the
/// HashSet key. Two sids of the same signature produce identical
/// bytes; cross-signature collisions require a 64-bit hash birthday
/// — we don't compress here, we compare the full bytes.
fn signature_bytes(
    metric_name: &str,
    agg_kind: &AggKind,
    group_by_keys: &BTreeSet<String>,
) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    signature_into(metric_name, agg_kind, group_by_keys, &mut buf);
    buf
}

/// Append the canonical signature encoding into `buf` (which the
/// caller may have pre-cleared and reuse across instances to avoid
/// per-sid allocation).
fn signature_into(
    metric_name: &str,
    agg_kind: &AggKind,
    group_by_keys: &BTreeSet<String>,
    buf: &mut Vec<u8>,
) {
    buf.extend_from_slice(metric_name.as_bytes());
    buf.push(0);
    encode_agg_kind(agg_kind, buf);
    buf.push(0);
    for k in group_by_keys {
        buf.extend_from_slice(k.as_bytes());
        buf.push(b',');
    }
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
    use crate::storage_engines::sketch_db::data::{SketchAlgorithm, SketchConfig};
    match agg_kind {
        AggKind::Sketch {
            algorithm: kind,
            config,
            spatial_filter_canonical,
        } => {
            buf.push(b'S');
            buf.push(match kind {
                SketchAlgorithm::DDSketch => 1,
                SketchAlgorithm::Kll => 2,
                SketchAlgorithm::Hll => 3,
                SketchAlgorithm::CountSketch => 4,
                SketchAlgorithm::Cms => 5,
                SketchAlgorithm::CmsWithHeap => 6,
                SketchAlgorithm::CountSketchWithHeap => 7,
                SketchAlgorithm::Kmv => 8,
                SketchAlgorithm::Theta => 9,
                SketchAlgorithm::UnivMon => 10,
            });
            match config {
                SketchConfig::UnivMon {
                    heap_size,
                    sketch_rows,
                    sketch_cols,
                    layers,
                } => {
                    buf.push(b'U');
                    buf.extend_from_slice(&heap_size.to_le_bytes());
                    buf.extend_from_slice(&sketch_rows.to_le_bytes());
                    buf.extend_from_slice(&sketch_cols.to_le_bytes());
                    buf.push(*layers);
                }
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
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;

    use crate::storage_engines::sketch_db::data::AggKind;
    use crate::storage_engines::sketch_db::index::{SketchInstanceMetadata, SketchStore};

    fn agg_config(
        metric: &str,
        agg_type: AggregationType,
        group_by: Vec<&str>,
    ) -> AggregationConfig {
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
            WindowKind::Tumbling,
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
        let group_by_keys: BTreeSet<String> = group_by.into_iter().map(|s| s.to_string()).collect();
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
        let cfg = streaming(vec![agg_config(
            "cpu",
            AggregationType::Sum,
            vec!["region"],
        )]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));
        assert_eq!(summary.retired, vec![1]);
    }

    #[test]
    fn already_retired_sid_is_not_re_retired() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        // First reconcile with empty config retires it.
        let _ =
            reconcile_from_streaming_config(&store, &streaming(vec![]), Duration::from_secs(60));
        // Second reconcile reports nothing new.
        let summary =
            reconcile_from_streaming_config(&store, &streaming(vec![]), Duration::from_secs(60));
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
        let cfg = streaming(vec![agg_config(
            "cpu",
            AggregationType::Increase,
            vec!["host"],
        )]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));
        assert_eq!(summary.retired, vec![1]);
    }

    fn meta_sketch(
        sid: u64,
        metric: &str,
        kind: crate::storage_engines::sketch_db::data::SketchAlgorithm,
        config: crate::storage_engines::sketch_db::data::SketchConfig,
        group_by: Vec<&str>,
    ) -> SketchInstanceMetadata {
        let group_by_keys: BTreeSet<String> = group_by.into_iter().map(|s| s.to_string()).collect();
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys,
            capability: None,
            agg_kind: AggKind::Sketch {
                algorithm: kind,
                config,
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    // Regression: the precompute-shaped signature reconcile must NOT
    // retire `AggKind::Sketch` sids. `build_live_signature_set` only ever
    // emits `ExactAgg`-shaped (`P`-prefixed) signatures from the streaming
    // config, so a sketch sid's `S`-prefixed signature can never be in the
    // live set. Before the fix this scan unconditionally orphaned every
    // OTLP-ingested KLL / HLL / DDSketch sid on the first reconcile, which
    // barred ingest, let eviction drop their state, and turned them into
    // query-time `Ghost`s that short-circuited warm quantile / cardinality
    // queries to CapabilityMiss. Sketch-sid lifecycle is the control
    // plane's eviction RPC's job, not this path's.
    #[test]
    fn sketch_sids_are_never_retired_by_signature_reconcile() {
        use crate::storage_engines::sketch_db::data::{SketchAlgorithm, SketchConfig};
        let store = SketchStore::new();
        store.register(meta_sketch(
            1,
            "http_requests_total_latency_ms",
            SketchAlgorithm::Kll,
            SketchConfig::Kll { k: 200 },
            vec!["node", "pod", "zone"],
        ));
        store.register(meta_sketch(
            2,
            "unique_users_per_min",
            SketchAlgorithm::Hll,
            SketchConfig::Hll { precision: 12 },
            vec!["zone"],
        ));
        // A precompute (ExactAgg) sid with no live config entry SHOULD
        // still retire — the fix is scoped to sketch sids only.
        store.register(meta(3, "cpu", AggregationType::Sum, vec!["host"]));

        // Live config carries an unrelated ExactAgg policy; none of the
        // sketch sids' signatures can match it.
        let cfg = streaming(vec![agg_config("mem", AggregationType::Sum, vec!["host"])]);
        let summary = reconcile_from_streaming_config(&store, &cfg, Duration::from_secs(60));

        // Only the orphaned precompute sid retires; both sketch sids stay
        // Active so the warm query path can keep resolving them.
        assert_eq!(summary.retired, vec![3], "only the ExactAgg orphan retires");
        assert!(
            matches!(
                store.classify(1),
                crate::storage_engines::sketch_db::index::SeriesLookup::Ghost
            ),
            "sketch sid 1 stays registered + Active (Ghost only because no data appended in-test)"
        );
        let inst1 = store.instance(1).expect("sketch sid 1 still registered");
        assert_eq!(
            inst1.status(),
            AggStatus::Active,
            "KLL sketch sid must remain Active after reconcile"
        );
        let inst2 = store.instance(2).expect("sketch sid 2 still registered");
        assert_eq!(
            inst2.status(),
            AggStatus::Active,
            "HLL sketch sid must remain Active after reconcile"
        );
    }

    #[test]
    fn gate_skips_reconcile_on_unchanged_config_arc() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));
        // Empty config would orphan sid 1.
        let cfg = Arc::new(streaming(vec![]));

        // First call against this Arc runs a real reconcile and
        // retires the orphan.
        let first = reconcile_if_config_changed(&store, &cfg, Duration::from_secs(60));
        assert_eq!(first.expect("first call reconciles").retired, vec![1]);

        // Second call with the SAME Arc is skipped entirely — no
        // catalog scan, returns None. (Even though a fresh
        // unconditional reconcile would report nothing, the point is
        // the scan didn't run.)
        let second = reconcile_if_config_changed(&store, &cfg, Duration::from_secs(60));
        assert!(second.is_none(), "same config Arc must skip the scan");
    }

    #[test]
    fn gate_runs_reconcile_when_config_arc_changes() {
        let store = SketchStore::new();
        store.register(meta(1, "cpu", AggregationType::Sum, vec!["host"]));

        // A config that keeps sid 1 alive — first reconcile retires
        // nothing and arms the gate.
        let keep = Arc::new(streaming(vec![agg_config(
            "cpu",
            AggregationType::Sum,
            vec!["host"],
        )]));
        let r1 = reconcile_if_config_changed(&store, &keep, Duration::from_secs(60));
        assert!(r1.expect("first reconcile runs").retired.is_empty());

        // A NEW Arc (different config) must defeat the gate and run a
        // fresh reconcile that now retires the orphaned sid.
        let drop_all = Arc::new(streaming(vec![]));
        let r2 = reconcile_if_config_changed(&store, &drop_all, Duration::from_secs(60));
        assert_eq!(r2.expect("changed config Arc reconciles").retired, vec![1]);
    }
}
