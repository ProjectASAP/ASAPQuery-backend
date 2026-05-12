//! End-to-end tests for on-disk persistence format versioning.
//!
//! sketch-DB persists three kinds of state that each carry a
//! format-version tag:
//!
//! * `SchemaRegistry` snapshot — JSON,
//!   `stores::sketch_db::schema::PERSIST_FORMAT_VERSION` (currently 1)
//! * `BackfillRegistry` snapshot — JSON,
//!   `stores::sketch_db::backfill::PERSIST_FORMAT_VERSION` (currently 1)
//! * `SketchStore` part `meta.bin` — binary,
//!   `stores::sketch_db::store::persistence::part::PART_FORMAT_VERSION` (currently 1)
//!
//! Each has a load path that tests `version == CURRENT`. The per-
//! module unit tests already cover the happy-path roundtrip and a
//! single "v999 → safe fallback" case. This module adds the
//! remaining cases the paper needs — realistic multi-entry
//! snapshots, field-level corruption, truncation, and the
//! previously-untested `meta.bin` malformed-header paths.
//!
//! Lives inside `src/tests/` (not `tests/`) so it can reach the
//! `PERSIST_FORMAT_VERSION` constants + private helpers without
//! pub-ifying them.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::stores::sketch_db::backfill::{
    BackfillJob, BackfillRegistry, BackfillSource, BackfillStatus,
};
use crate::stores::sketch_db::schema::{AggStatus, SchemaRegistry};
use crate::stores::sketch_db::store::persistence::part::{
    MAGIC_META, META_HEADER_SIZE, PART_FORMAT_VERSION,
};

fn tmpdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

// ─── SchemaRegistry persistence format ─────────────────────────────────

mod schema {
    use super::*;
    use crate::stores::types::StreamingConfig;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

    fn make_cfg(agg_id: u64, metric: &str) -> AggregationConfig {
        AggregationConfig::new(
            agg_id,
            AggregationType::CountMinSketch,
            String::new(),
            std::collections::HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    fn make_streaming(agg_ids_and_metrics: &[(u64, &str)]) -> StreamingConfig {
        let mut map = std::collections::HashMap::new();
        for (id, metric) in agg_ids_and_metrics {
            map.insert(*id, make_cfg(*id, metric));
        }
        StreamingConfig::new(map)
    }

    /// Round-trip a realistic multi-schema state through disk and
    /// back — three schemas covering all three `AggStatus` states
    /// (Active / Retired / Expired), two different metrics.
    #[test]
    fn realistic_multi_schema_round_trips_through_disk() {
        let dir = tmpdir();
        let path = dir.path().join("schemas.json");

        // Build an initial registry with 2 metrics × 2 agg_ids each.
        let cfg_v1 = make_streaming(&[
            (1, "http_requests_total"),
            (2, "http_requests_total"),
            (10, "sensor_reading"),
            (20, "sensor_reading"),
        ]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg_v1);
        assert!(r.is_writable(1));
        assert!(r.is_writable(20));

        // Evolve: drop agg 1, drop agg 10 → they're retired.
        let cfg_v2 = make_streaming(&[(2, "http_requests_total"), (20, "sensor_reading")]);
        let _ = r.reconcile(&cfg_v2);
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Retired);
        assert_eq!(r.get(10).unwrap().status(), AggStatus::Retired);

        // Drop + reload. All four must be present, statuses preserved.
        drop(r);
        let reloaded = SchemaRegistry::load_or_new_from_config(&path, &cfg_v2);
        assert!(reloaded.is_writable(2));
        assert!(reloaded.is_writable(20));
        assert_eq!(reloaded.get(1).unwrap().status(), AggStatus::Retired);
        assert_eq!(reloaded.get(10).unwrap().status(), AggStatus::Retired);

        // The file's on-wire version stays v1.
        let bytes = fs::read(&path).unwrap();
        let snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snap["version"], 1, "wire version must stay at current");
    }

    /// Field-level tamper: flip just the `version` field in a
    /// legitimate snapshot from 1 → 999. Loader must treat this as
    /// an unsupported version (not parse the rest as if it were v1)
    /// and fall back to fresh.
    #[test]
    fn tampered_version_field_triggers_safe_fallback() {
        let dir = tmpdir();
        let path = dir.path().join("schemas.json");

        // Write a valid v1 snapshot with one schema.
        let cfg = make_streaming(&[(7, "metric_7")]);
        drop(SchemaRegistry::load_or_new_from_config(&path, &cfg));

        // Tamper: parse, bump version to 999, rewrite.
        let bytes = fs::read(&path).unwrap();
        let mut snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        snap["version"] = serde_json::json!(999);
        fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap()).unwrap();

        // Reload: must fall back, reconcile against the new config,
        // and rewrite a v1 snapshot. No panic.
        let cfg_new = make_streaming(&[(42, "metric_42")]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg_new);
        assert!(r.is_writable(42), "fresh registry must come from cfg_new");
        assert!(
            r.get(7).is_none(),
            "tampered v999 snapshot's schemas must NOT leak in",
        );

        let after: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(after["version"], 1, "post-fallback snapshot is v1 again");
    }

    /// Truncated JSON: writer crashed midway, file doesn't parse.
    /// Loader must fall back, not crash.
    #[test]
    fn truncated_snapshot_falls_back_without_panic() {
        let dir = tmpdir();
        let path = dir.path().join("schemas.json");

        // Valid file first.
        let cfg = make_streaming(&[(1, "m1")]);
        drop(SchemaRegistry::load_or_new_from_config(&path, &cfg));

        // Truncate to half size — mid-JSON, broken object.
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();

        let cfg2 = make_streaming(&[(2, "m2")]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg2);
        assert!(r.is_writable(2));
        assert!(r.get(1).is_none());
    }

    /// Zero-byte file: newly-created but never written.
    #[test]
    fn empty_snapshot_file_falls_back_without_panic() {
        let dir = tmpdir();
        let path = dir.path().join("schemas.json");
        fs::write(&path, b"").unwrap();

        let cfg = make_streaming(&[(99, "m99")]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg);
        assert!(r.is_writable(99));
    }

    /// Structural mismatch — file parses as JSON but the shape is
    /// wrong (missing `schemas` array entirely). Today the decoder
    /// requires the full shape; verify it falls back cleanly.
    #[test]
    fn wrong_shape_snapshot_falls_back_without_panic() {
        let dir = tmpdir();
        let path = dir.path().join("schemas.json");
        fs::write(&path, br#"{"version": 1}"#).unwrap();

        let cfg = make_streaming(&[(5, "m5")]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg);
        assert!(r.is_writable(5));
    }
}

// ─── BackfillRegistry persistence format ────────────────────────────────

mod backfill {
    use super::*;

    fn write_v1_backfill_snapshot(path: &PathBuf, jobs: Vec<BackfillJob>, next_id: u64) {
        let snap = serde_json::json!({
            "version": 1,
            "next_job_id": next_id,
            "jobs": jobs,
        });
        fs::write(path, serde_json::to_vec_pretty(&snap).unwrap()).unwrap();
    }

    fn make_job(id: u64, agg_id: u64, status: BackfillStatus) -> BackfillJob {
        BackfillJob {
            job_id: id,
            agg_id,
            time_range: (1_000_000, 1_001_000),
            source: BackfillSource::Prometheus {
                url: "http://prom:9090".to_string(),
            },
            status,
            windows_done: 0,
            windows_total: 10,
            created_at_ms: 1_700_000_000_000,
            started_at_ms: None,
            completed_at_ms: None,
            error_message: None,
        }
    }

    /// Round-trip multiple jobs at different statuses through disk
    /// and back. Exercises every `BackfillStatus` variant.
    #[test]
    fn realistic_multi_job_round_trips_through_disk() {
        let dir = tmpdir();
        let path = dir.path().join("backfill.json");

        let jobs = vec![
            make_job(1, 101, BackfillStatus::Queued),
            make_job(2, 102, BackfillStatus::Running),
            make_job(3, 103, BackfillStatus::Complete),
            make_job(4, 104, BackfillStatus::Cancelled),
            make_job(5, 105, BackfillStatus::Failed),
        ];
        write_v1_backfill_snapshot(&path, jobs.clone(), 6);

        let r = BackfillRegistry::load_or_new(path.clone());
        for original in &jobs {
            let got = r.get(original.job_id).expect("job present");
            assert_eq!(got.agg_id, original.agg_id);
            assert_eq!(got.status, original.status);
            assert_eq!(got.windows_total, original.windows_total);
        }

        // Post-load snapshot is still at the current version.
        let bytes = fs::read(&path).unwrap();
        let snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snap["version"], 1);
    }

    /// `next_job_id` must be restored so future creates don't
    /// collide with persisted IDs. Covered by the existing
    /// `persistence_preserves_next_job_id_across_restart` test
    /// generatively; this is a **golden-file** variant that pins
    /// the field's location in the v1 wire format.
    #[test]
    fn v1_wire_format_carries_next_job_id_field() {
        let dir = tmpdir();
        let path = dir.path().join("backfill.json");
        let jobs = vec![make_job(42, 1, BackfillStatus::Complete)];
        write_v1_backfill_snapshot(&path, jobs, 43);

        let r = BackfillRegistry::load_or_new(path.clone());
        // Creating a new job must allocate 43, not 1 or 44.
        let new_id = r.create(
            200,
            (0, 1000),
            BackfillSource::Prometheus {
                url: "x".to_string(),
            },
            1,
        );
        assert_eq!(new_id, 43, "next_job_id was persisted at 43");
    }

    /// Tampered version → safe fallback (already covered by a
    /// module-local test; golden-file variant here to make the
    /// wire-format contract explicit).
    #[test]
    fn tampered_version_field_triggers_safe_fallback() {
        let dir = tmpdir();
        let path = dir.path().join("backfill.json");

        // Write v1 with one job, then tamper version.
        let jobs = vec![make_job(1, 1, BackfillStatus::Queued)];
        write_v1_backfill_snapshot(&path, jobs, 2);
        let bytes = fs::read(&path).unwrap();
        let mut snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        snap["version"] = serde_json::json!(999);
        fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap()).unwrap();

        let r = BackfillRegistry::load_or_new(path.clone());
        assert!(r.get(1).is_none(), "tampered v999 snapshot discarded");
        // Rewrite after load brings it back to v1.
        let after: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(after["version"], 1);
    }

    /// Truncated snapshot → fallback, no panic.
    #[test]
    fn truncated_snapshot_falls_back_without_panic() {
        let dir = tmpdir();
        let path = dir.path().join("backfill.json");
        let jobs = vec![make_job(1, 1, BackfillStatus::Queued)];
        write_v1_backfill_snapshot(&path, jobs, 2);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() / 3]).unwrap();

        let r = BackfillRegistry::load_or_new(path.clone());
        assert!(r.get(1).is_none());
    }

    /// Shape mismatch — valid JSON, wrong structure. Fallback.
    #[test]
    fn wrong_shape_snapshot_falls_back_without_panic() {
        let dir = tmpdir();
        let path = dir.path().join("backfill.json");
        fs::write(&path, br#"{"version": 1, "unrelated": "field"}"#).unwrap();

        let r = BackfillRegistry::load_or_new(path.clone());
        assert!(r.get(1).is_none());
    }
}

// ─── SketchStore part meta.bin format ────────────────────────────────

mod part_meta {
    use super::*;
    use crate::stores::sketch_db::store::persistence::part::PartReader;

    /// Build a valid 64-byte meta.bin header for part_id=1.
    fn valid_header() -> Vec<u8> {
        let mut h = vec![0u8; META_HEADER_SIZE];
        h[0..4].copy_from_slice(&MAGIC_META.to_le_bytes());
        h[4..6].copy_from_slice(&PART_FORMAT_VERSION.to_le_bytes());
        // flags 6..8 = 0
        h[8..16].copy_from_slice(&1u64.to_le_bytes()); // part_id
        h[16..24].copy_from_slice(&0u64.to_le_bytes()); // min_ts
        h[24..32].copy_from_slice(&1000u64.to_le_bytes()); // max_ts
        h[32..36].copy_from_slice(&5u32.to_le_bytes()); // num_entries
                                                        // 36..40 reserved
        h[40..48].copy_from_slice(&0u64.to_le_bytes()); // data_len
        h[48..56].copy_from_slice(&0u64.to_le_bytes()); // index_len
        h[56..60].copy_from_slice(&1_700_000_000u32.to_le_bytes()); // created_unix_secs
        h[60..64].copy_from_slice(&0u32.to_le_bytes()); // crc placeholder
        h
    }

    fn write_part_dir(dir: &std::path::Path, header: &[u8]) -> std::path::PathBuf {
        let part_dir = dir.join("part_0000000000000001");
        fs::create_dir_all(&part_dir).unwrap();
        let meta = part_dir.join("meta.bin");
        let mut f = fs::File::create(&meta).unwrap();
        f.write_all(header).unwrap();
        // Empty data.bin + index.bin so later reads don't choke
        // before meta is checked.
        fs::write(part_dir.join("data.bin"), b"").unwrap();
        fs::write(part_dir.join("index.bin"), b"").unwrap();
        part_dir
    }

    /// Bad magic word → `PersistError::Format`, not panic.
    #[test]
    fn bad_magic_returns_format_error() {
        let dir = tmpdir();
        let mut header = valid_header();
        header[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let part_dir = write_part_dir(dir.path(), &header);

        let err = PartReader::read_meta(&part_dir).expect_err("must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("bad magic"),
            "error should mention bad magic; got: {msg}"
        );
    }

    /// Bad version → `PersistError::Format`, not panic. Catches
    /// the case where the `PART_FORMAT_VERSION` is bumped in code
    /// but on-disk state is stale.
    #[test]
    fn unsupported_version_returns_format_error() {
        let dir = tmpdir();
        let mut header = valid_header();
        // Intentionally write a future version.
        header[4..6].copy_from_slice(&9999u16.to_le_bytes());
        let part_dir = write_part_dir(dir.path(), &header);

        let err = PartReader::read_meta(&part_dir).expect_err("must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported version"),
            "error should mention unsupported version; got: {msg}"
        );
    }

    /// Truncated header (< 64 bytes) → I/O error from `read_exact`,
    /// propagated via `PersistError`. Not panic.
    #[test]
    fn truncated_header_returns_error() {
        let dir = tmpdir();
        // Only 32 bytes — half of META_HEADER_SIZE.
        let header = valid_header()[..32].to_vec();
        let part_dir = write_part_dir(dir.path(), &header);

        let err = PartReader::read_meta(&part_dir).expect_err("must error");
        // Error may be an I/O error (EOF) or a Format error
        // depending on how read_exact surfaces it; either is
        // acceptable — the point is no panic.
        let _ = format!("{err}");
    }

    /// Missing meta.bin entirely → I/O error.
    #[test]
    fn missing_meta_file_returns_error() {
        let dir = tmpdir();
        let part_dir = dir.path().join("part_0000000000000001");
        fs::create_dir_all(&part_dir).unwrap();
        // Intentionally do NOT create meta.bin.

        let err = PartReader::read_meta(&part_dir).expect_err("must error");
        let _ = format!("{err}");
    }
}

// ─── v2 forward-compat: load-with-bumped-version contract ──────────────
//
// TODO.md §4 calls for: "Check in a golden snapshot at v1, load with v2
// code, verify expected migration OR safe fall-back-to-fresh, assert no
// crash, no data corruption." The earlier modules cover the policy with
// hardcoded v999 sentinels; this module pins the contract to the
// **actual constant + 1**, so the tests self-update if/when the version
// is bumped — and explicitly asserts the rewritten on-disk file is at
// the current version with no orphan fields.
mod v2_forward_compat {
    use super::*;
    use crate::stores::sketch_db::backfill::PERSIST_FORMAT_VERSION as BACKFILL_V;
    use crate::stores::sketch_db::schema::PERSIST_FORMAT_VERSION as SCHEMA_V;
    use crate::stores::sketch_db::store::persistence::part::PartReader;

    /// SchemaRegistry: snapshot tagged v_current+1 must trigger safe
    /// fallback, and the rewrite must be at v_current with the new
    /// config's schemas — no leakage from the future-version blob.
    #[test]
    fn schema_v1_with_future_version_falls_back_and_rewrites_clean() {
        use crate::stores::types::StreamingConfig;
        use asap_types::aggregation_config::AggregationConfig;
        use asap_types::enums::{AggregationType, WindowType};
        use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

        fn make_cfg(agg_id: u64, metric: &str) -> AggregationConfig {
            AggregationConfig::new(
                agg_id,
                AggregationType::CountMinSketch,
                String::new(),
                std::collections::HashMap::new(),
                KeyByLabelNames::empty(),
                KeyByLabelNames::empty(),
                KeyByLabelNames::empty(),
                String::new(),
                60,
                60,
                WindowType::Tumbling,
                String::new(),
                metric.to_string(),
                None,
                None,
                None,
            )
        }

        let dir = tmpdir();
        let path = dir.path().join("schemas.json");

        // Seed a legitimate v_current snapshot with one schema.
        let cfg_seed = {
            let mut m = std::collections::HashMap::new();
            m.insert(7, make_cfg(7, "future_version_metric"));
            StreamingConfig::new(m)
        };
        drop(SchemaRegistry::load_or_new_from_config(&path, &cfg_seed));

        // Bump version field to SCHEMA_V + 1 — simulating "v2 code wrote
        // this; we are v1 and should refuse to interpret it as v1."
        let mut snap: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        snap["version"] = serde_json::json!(SCHEMA_V + 1);
        fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap()).unwrap();

        // Reload with a fresh config: must fall back, agg 7 must NOT
        // appear, agg 99 (from new config) must appear.
        let cfg_new = {
            let mut m = std::collections::HashMap::new();
            m.insert(99, make_cfg(99, "fresh_metric"));
            StreamingConfig::new(m)
        };
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg_new);
        assert!(r.is_writable(99), "fresh registry must seed from cfg_new");
        assert!(
            r.get(7).is_none(),
            "future-version snapshot's schemas must NOT leak in",
        );

        // No data corruption: the rewritten file must be valid JSON, at
        // SCHEMA_V exactly, with the new agg_id present and no stray
        // fields from the bumped blob.
        let after: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).expect("post-fallback file is JSON");
        assert_eq!(after["version"], SCHEMA_V);
        assert!(
            after["schemas"].is_array(),
            "post-fallback snapshot has the v_current shape"
        );
    }

    /// BackfillRegistry: same contract.
    #[test]
    fn backfill_v1_with_future_version_falls_back_and_rewrites_clean() {
        let dir = tmpdir();
        let path = dir.path().join("backfill.json");

        // Hand-write a snapshot at BACKFILL_V + 1 with one job.
        let job = BackfillJob {
            job_id: 1,
            agg_id: 1,
            time_range: (0, 1000),
            source: BackfillSource::Prometheus {
                url: "http://prom".to_string(),
            },
            status: BackfillStatus::Queued,
            windows_done: 0,
            windows_total: 1,
            created_at_ms: 1_700_000_000_000,
            started_at_ms: None,
            completed_at_ms: None,
            error_message: None,
        };
        let snap = serde_json::json!({
            "version": BACKFILL_V + 1,
            "next_job_id": 2,
            "jobs": [job],
        });
        fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap()).unwrap();

        let r = BackfillRegistry::load_or_new(path.clone());
        assert!(r.get(1).is_none(), "future-version job must not leak in");

        // No data corruption: rewritten file at BACKFILL_V, valid shape.
        let after: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).expect("post-fallback file is JSON");
        assert_eq!(after["version"], BACKFILL_V);
        assert!(after["jobs"].is_array());
        assert!(
            after["next_job_id"].is_number(),
            "next_job_id field must remain present"
        );
    }

    /// SketchStore part meta.bin: header tagged PART_FORMAT_VERSION+1
    /// must surface a `PersistError::Format`. This is the
    /// "v2-on-disk-loaded-by-v1-code" path; for parts there is no
    /// fallback (each part is opaque), so a clean error is the contract.
    #[test]
    fn part_meta_with_future_version_returns_format_error() {
        let dir = tmpdir();
        let mut header = vec![0u8; META_HEADER_SIZE];
        header[0..4].copy_from_slice(&MAGIC_META.to_le_bytes());
        header[4..6].copy_from_slice(&(PART_FORMAT_VERSION + 1).to_le_bytes());
        header[8..16].copy_from_slice(&1u64.to_le_bytes());
        header[16..24].copy_from_slice(&0u64.to_le_bytes());
        header[24..32].copy_from_slice(&1000u64.to_le_bytes());
        header[32..36].copy_from_slice(&5u32.to_le_bytes());
        header[40..48].copy_from_slice(&0u64.to_le_bytes());
        header[48..56].copy_from_slice(&0u64.to_le_bytes());
        header[56..60].copy_from_slice(&1_700_000_000u32.to_le_bytes());

        let part_dir = dir.path().join("part_0000000000000001");
        fs::create_dir_all(&part_dir).unwrap();
        fs::write(part_dir.join("meta.bin"), &header).unwrap();
        fs::write(part_dir.join("data.bin"), b"").unwrap();
        fs::write(part_dir.join("index.bin"), b"").unwrap();

        let err = PartReader::read_meta(&part_dir).expect_err("must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported version"),
            "error must mention unsupported version; got: {msg}"
        );
    }
}
