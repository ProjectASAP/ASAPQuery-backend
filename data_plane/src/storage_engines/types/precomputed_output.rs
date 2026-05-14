use asap_types::PolicyFingerprint;
use serde::{Deserialize, Serialize};

use crate::storage_engines::types::KeyByLabelValues;

/// Provenance tag stamped on every `PrecomputedOutput`: did this
/// window come from live ingest or was it materialised by a backfill
/// job?
///
/// `Native` is the default (records without the field deserialise as
/// `Native` via `#[serde(default)]`), so the tag is forward-compatible
/// with older on-disk formats.
///
/// Read-side consumers (HTTP listing of backfilled windows, coverage
/// UI, audit logs) belong to the backfill subsystem under
/// [`crate::storage_engines::sketch_db::backfill`]; today this field is
/// written but no production read site branches on it yet. Recording
/// it eagerly means a window written before the read-side consumer
/// lands still carries its `job_id` — readers can recover history,
/// not just future.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum Origin {
    /// Emitted by the live ingest pipeline (the
    /// `PrecomputeEngine` workers closing a window). The common case.
    #[default]
    Native,
    /// Materialised by a backfill job. The tag carries the `job_id`
    /// so operators can trace back to the specific REFRESH run that
    /// produced this window (`GET /api/v1/db/backfill/jobs/:id`).
    Backfilled { job_id: u64 },
}

/// One window of one (sid, label-values) group's emitted aggregation
/// state. Produced by `PrecomputeWorker` / `BackfillWindowProcessor`
/// and consumed by `SketchStoreSink::append_to_index`.
///
/// **PR-6 follow-up:** the `aggregation_id: u64` field that this struct
/// used to carry alongside `policy_fp` is gone — `policy_fp` is the
/// only identity handle. PR 4 added `policy_fp` as a parallel field
/// with `aggregation_id` kept for legacy compat; PR 5 retired
/// `AggregationConfig::aggregation_id`; this PR completes the cleanup
/// by retiring the field on `PrecomputedOutput` too. Sinks resolve the
/// source config via `PolicyRegistry::get(policy_fp)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputedOutput {
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub key: Option<KeyByLabelValues>,
    /// Provenance tag. `#[serde(default)]` on read means records
    /// without the field deserialise as `Native`, preserving
    /// forward-compat with older on-disk payloads.
    #[serde(default)]
    pub origin: Origin,
    /// Content-addressed policy identity. The data plane's only handle
    /// on which source `AggregationConfig` produced this output.
    /// `#[serde(default)]` on read preserves forward-compat with
    /// PR-3 / PR-4-era records that may not have carried the field;
    /// sinks treat `PolicyFingerprint::UNSET` as "skip this output"
    /// (matches the raw-mode fast path where no source config exists).
    #[serde(default)]
    pub policy_fp: PolicyFingerprint,
}

impl PrecomputedOutput {
    /// Construct a `Native` precompute.
    ///
    /// `policy_fp` is the content-addressed handle on the source
    /// [`asap_types::AggregationConfig`]; sinks use it to look up the
    /// config via `PolicyRegistry::get(policy_fp)`. Construction sites
    /// that lack a source config (raw-mode fast-path) pass
    /// [`PolicyFingerprint::UNSET`]; sinks then skip the output.
    pub fn new(
        start_timestamp: u64,
        end_timestamp: u64,
        key: Option<KeyByLabelValues>,
        policy_fp: PolicyFingerprint,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            key,
            origin: Origin::Native,
            policy_fp,
        }
    }

    /// Construct a `Backfilled { job_id }` precompute. Called by
    /// [`crate::storage_engines::sketch_db::backfill::processor::BackfillWindowProcessor`]
    /// so each backfilled window carries its provenance back to the
    /// originating `BackfillJob`.
    pub fn new_backfilled(
        start_timestamp: u64,
        end_timestamp: u64,
        key: Option<KeyByLabelValues>,
        job_id: u64,
        policy_fp: PolicyFingerprint,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            key,
            origin: Origin::Backfilled { job_id },
            policy_fp,
        }
    }

    pub fn get_freshness_debug_string(&self) -> String {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let freshness = current_time.saturating_sub(self.end_timestamp);
        format!(
            "end_timestamp: {}, current_time: {}, freshness: {}",
            self.end_timestamp, current_time, freshness
        )
    }
}

// ─── Origin tag tests ─────────────────────────────────────────────

#[cfg(test)]
mod origin_tests {
    use super::*;

    #[test]
    fn default_origin_is_native() {
        assert_eq!(Origin::default(), Origin::Native);
    }

    #[test]
    fn new_constructor_produces_native_origin() {
        let out = PrecomputedOutput::new(0, 100, None, PolicyFingerprint(1));
        assert_eq!(out.origin, Origin::Native);
    }

    #[test]
    fn new_backfilled_constructor_carries_job_id() {
        let out = PrecomputedOutput::new_backfilled(0, 100, None, 42, PolicyFingerprint(1));
        assert_eq!(out.origin, Origin::Backfilled { job_id: 42 });
    }

    #[test]
    fn serde_roundtrip_preserves_native() {
        let out = PrecomputedOutput::new(10, 20, None, PolicyFingerprint(7));
        let json = serde_json::to_string(&out).unwrap();
        let back: PrecomputedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.origin, Origin::Native);
        assert_eq!(back.policy_fp, PolicyFingerprint(7));
    }

    #[test]
    fn serde_roundtrip_preserves_backfilled_job_id() {
        let out = PrecomputedOutput::new_backfilled(10, 20, None, 99, PolicyFingerprint(7));
        let json = serde_json::to_string(&out).unwrap();
        let back: PrecomputedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.origin, Origin::Backfilled { job_id: 99 });
        assert_eq!(back.policy_fp, PolicyFingerprint(7));
    }

    /// Forward-compat guard: pre-this-PR records lack the `policy_fp`
    /// field and may still carry the retired `aggregation_id` field;
    /// `#[serde(default)]` on `policy_fp` + serde's
    /// ignore-unknown-fields default means such records deserialise
    /// with `policy_fp = UNSET`. Sinks treat that as "skip" — same
    /// behaviour as the raw-mode path that never had a source config.
    #[test]
    fn old_serialized_record_without_policy_fp_deserialises_as_unset() {
        let old_json = r#"{
            "start_timestamp": 100,
            "end_timestamp": 200,
            "key": null,
            "aggregation_id": 3
        }"#;
        let out: PrecomputedOutput = serde_json::from_str(old_json).unwrap();
        assert_eq!(out.origin, Origin::Native);
        assert!(out.policy_fp.is_unset());
    }

    #[test]
    fn origin_variants_are_not_equal() {
        assert_ne!(Origin::Native, Origin::Backfilled { job_id: 1 });
        assert_ne!(
            Origin::Backfilled { job_id: 1 },
            Origin::Backfilled { job_id: 2 }
        );
    }
}
