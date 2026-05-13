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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputedOutput {
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub key: Option<KeyByLabelValues>,
    pub aggregation_id: u64,
    /// Provenance tag. `#[serde(default)]` on read means records
    /// without the field deserialise as `Native`, preserving
    /// forward-compat with older on-disk payloads.
    #[serde(default)]
    pub origin: Origin,
}

impl PrecomputedOutput {
    /// Construct a `Native` precompute — the default used by the
    /// live ingest pipeline.
    pub fn new(
        start_timestamp: u64,
        end_timestamp: u64,
        key: Option<KeyByLabelValues>,
        aggregation_id: u64,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            key,
            aggregation_id,
            origin: Origin::Native,
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
        aggregation_id: u64,
        job_id: u64,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            key,
            aggregation_id,
            origin: Origin::Backfilled { job_id },
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
        let out = PrecomputedOutput::new(0, 100, None, 1);
        assert_eq!(out.origin, Origin::Native);
    }

    #[test]
    fn new_backfilled_constructor_carries_job_id() {
        let out = PrecomputedOutput::new_backfilled(0, 100, None, 1, 42);
        assert_eq!(out.origin, Origin::Backfilled { job_id: 42 });
    }

    #[test]
    fn serde_roundtrip_preserves_native() {
        let out = PrecomputedOutput::new(10, 20, None, 7);
        let json = serde_json::to_string(&out).unwrap();
        let back: PrecomputedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.origin, Origin::Native);
    }

    #[test]
    fn serde_roundtrip_preserves_backfilled_job_id() {
        let out = PrecomputedOutput::new_backfilled(10, 20, None, 7, 99);
        let json = serde_json::to_string(&out).unwrap();
        let back: PrecomputedOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.origin, Origin::Backfilled { job_id: 99 });
    }

    /// Forward-compat guard: old on-disk records without the
    /// `origin` field must deserialise as `Native`, not fail with
    /// "missing field". Matters because the persisted payload format
    /// predates the addition of `origin`.
    #[test]
    fn old_serialized_record_without_origin_deserialises_as_native() {
        let old_json = r#"{
            "start_timestamp": 100,
            "end_timestamp": 200,
            "key": null,
            "aggregation_id": 3
        }"#;
        let out: PrecomputedOutput = serde_json::from_str(old_json).unwrap();
        assert_eq!(out.origin, Origin::Native);
        assert_eq!(out.aggregation_id, 3);
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
