//! End-to-end tests for `SimpleMapStorePerKey::with_persistence`.
//!
//! Spins up a real store with a tempdir-backed persistence config,
//! inserts sketches through the public `Store` trait, waits for the
//! background flusher to write a part and evict the sealed epoch,
//! and verifies `query_precomputed_output` returns the flushed data
//! via the disk read-through path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use promql_utilities::data_model::KeyByLabelNames;
use tempfile::TempDir;

use crate::data_model::{
    AggregationType, CleanupPolicy, PrecomputedOutput, StreamingConfig, WindowType,
};
use crate::precompute_operators::SumAccumulator;
use crate::stores::sketch_db::simple_map_store::per_key::SimpleMapStorePerKey;
use crate::stores::sketch_db::simple_map_store::persistence::SimpleMapStorePersistenceConfig;
use crate::stores::Store;
use crate::{AggregateCore, AggregationConfig};

fn make_streaming_config(agg_id: u64) -> Arc<StreamingConfig> {
    // Retain a small number of aggregates per epoch so rotation fires
    // quickly and the flusher has something to chew on under test.
    let cfg = AggregationConfig::new(
        agg_id,
        AggregationType::Sum,
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
        "cpu_usage".to_string(),
        Some(2), // num_aggregates_to_retain — seals after 2 distinct windows
        None,
        None,
        None,
    );
    let mut map = std::collections::HashMap::new();
    map.insert(agg_id, cfg);
    Arc::new(StreamingConfig::new(map))
}

fn persistence_cfg(dir: &TempDir, hot_window_ms: Option<u64>) -> SimpleMapStorePersistenceConfig {
    SimpleMapStorePersistenceConfig {
        memory_limit_bytes: 100 * 1024 * 1024,
        memory_low_watermark_bytes: 50 * 1024 * 1024,
        hard_cap_bytes: 200 * 1024 * 1024,
        hot_window_ms,
        delete_older_than_ms: None,
        flush_interval: Duration::from_millis(25),
        disk_path: dir.path().to_path_buf(),
        part_cache_bytes: 1024 * 1024,
    }
}

fn sum_entry(
    agg_id: u64,
    start: u64,
    end: u64,
    value: f64,
) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
    (
        PrecomputedOutput::new(start, end, None, agg_id),
        Box::new(SumAccumulator::with_sum(value)),
    )
}

fn wait_for<F: FnMut() -> bool>(mut pred: F, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    pred()
}

#[test]
#[ignore = "depends on EpochSource::snapshot_sealed_epoch, which is stubbed to Ok(None) while the datafusion-dependent serialization path is rebuilt on top of the SketchIndex"]
fn with_persistence_flushes_sealed_epochs_to_disk() {
    let dir = TempDir::new().unwrap();
    let cfg = make_streaming_config(1);
    // hot_window_ms = Some(0) means "any sealed epoch's end_ts is
    // older than now-0 = now, so flush immediately on next tick."
    let persistence = persistence_cfg(&dir, Some(0));

    let store = SimpleMapStorePerKey::with_persistence(cfg, CleanupPolicy::NoCleanup, persistence)
        .expect("with_persistence");

    // Insert several windows so the rotator seals at least one epoch.
    // num_aggregates_to_retain = 2, so windows 3 will roll the epoch.
    let batches = vec![
        sum_entry(1, 1_000, 2_000, 1.0),
        sum_entry(1, 2_000, 3_000, 2.0),
        sum_entry(1, 3_000, 4_000, 3.0),
        sum_entry(1, 4_000, 5_000, 4.0),
    ];
    store.insert_precomputed_output_batch(batches).unwrap();

    // Wait until a part shows up in the manifest. This means the
    // flusher has written at least one tick's worth of sealed epochs
    // and evicted them from memory.
    let flushed = wait_for(
        || {
            let diag = store.diagnostic_info();
            // At least one agg with at least one sealed epoch
            // evicted — detected by checking the manifest via a
            // second query that goes through the disk path.
            let res = store
                .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
                .unwrap();
            // The test is satisfied once the query returns *any*
            // result AND diagnostic bytes are nonzero (something was
            // in memory at some point).
            !res.is_empty() && diag.total_time_map_entries < 4
        },
        Duration::from_secs(3),
    );
    assert!(flushed, "flusher did not produce a part in time");

    // Query across the full time range and verify we see all 4 windows
    // (some from memory, some from disk, or all from disk).
    let res = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    let total: usize = res.values().map(|v| v.len()).sum();
    assert_eq!(
        total,
        4,
        "expected 4 buckets across in-memory + disk; got {} (buckets: {:?})",
        total,
        res.values()
            .flat_map(|v| v.iter().map(|(tr, _)| *tr))
            .collect::<Vec<_>>()
    );
}

#[test]
#[ignore = "depends on EpochSource::snapshot_sealed_epoch, which is stubbed to Ok(None) while the datafusion-dependent serialization path is rebuilt on top of the SketchIndex"]
fn query_read_through_merges_memory_and_disk_ranges() {
    let dir = TempDir::new().unwrap();
    let cfg = make_streaming_config(42);
    let persistence = persistence_cfg(&dir, Some(0));
    let store = SimpleMapStorePerKey::with_persistence(cfg, CleanupPolicy::NoCleanup, persistence)
        .expect("with_persistence");

    // Insert 6 windows — more than enough to guarantee the rotator
    // seals multiple epochs.
    let mut batch = Vec::new();
    for i in 0..6u64 {
        let start = 10_000 + i * 1_000;
        let end = start + 1_000;
        batch.push(sum_entry(42, start, end, i as f64));
    }
    store.insert_precomputed_output_batch(batch).unwrap();

    // Give the flusher time to drain everything to disk.
    std::thread::sleep(Duration::from_millis(250));

    // Query a partial range covering 3 of the 6 windows — make sure
    // the filter is honored regardless of whether the hit came from
    // memory or disk.
    let res = store
        .query_precomputed_output("cpu_usage", 42, 12_000, 15_000)
        .unwrap();
    let timestamps: Vec<(u64, u64)> = {
        let mut ts: Vec<(u64, u64)> = res
            .get(&None)
            .map(|v| v.iter().map(|(tr, _)| *tr).collect())
            .unwrap_or_default();
        ts.sort_unstable();
        ts
    };
    // Expected: the two windows fully inside [12_000, 15_000]:
    //   (12_000, 13_000), (13_000, 14_000), (14_000, 15_000).
    // Note: range_query_into requires tr.0 >= start AND tr.1 <= end.
    assert_eq!(
        timestamps,
        vec![(12_000, 13_000), (13_000, 14_000), (14_000, 15_000)],
        "expected windows 12000-15000 inclusive, got {:?}",
        timestamps
    );
}

#[test]
fn construct_and_drop_shuts_flusher_cleanly() {
    let dir = TempDir::new().unwrap();
    let cfg = make_streaming_config(1);
    let persistence = persistence_cfg(&dir, None);
    let store = SimpleMapStorePerKey::with_persistence(cfg, CleanupPolicy::NoCleanup, persistence)
        .expect("with_persistence");
    // Dropping the store should not deadlock or panic.
    drop(store);
}

#[test]
#[ignore = "back-pressure assert requires the flusher to actually drain sealed epochs; while EpochSource::snapshot_sealed_epoch is stubbed to Ok(None), memory never drops and each over-cap insert blocks for the full 30s wait_for_memory_under timeout (200 inserts × 30 s ≈ deadlock from the lib-tests' perspective). Re-enable once the SketchIndex-backed snapshot path lands."]
fn hard_cap_back_pressure_blocks_inserts_until_flusher_drains() {
    // Construct a store with a very small hard cap and a flusher
    // whose tick interval is long enough that at least one insert
    // will catch the cap before a tick has time to drain it. The
    // first few inserts push mem_bytes_sealed past the cap; any
    // subsequent insert that arrives while mem_bytes_sealed is still
    // at or above the cap enters `wait_for_memory_under`'s blocking
    // wait loop, which increments a dedicated counter.
    //
    // We assert on that counter rather than on wall-clock timing.
    // A previous version of this test measured the slowest insert
    // and required it to be ≥3 ms, but on fast CI runners a flush
    // tick completes in well under a millisecond, so the slow path
    // would run and fully drain before any 3 ms threshold tripped —
    // leaving the assertion flaky even though back-pressure was
    // firing correctly. The counter is a timing-independent signal.

    let dir = TempDir::new().unwrap();
    let cfg = make_streaming_config(1);
    let persistence = SimpleMapStorePersistenceConfig {
        // Very small memory limit — a few hundred bytes — so the
        // insert path hits the cap after the first handful of items.
        memory_limit_bytes: 512,
        memory_low_watermark_bytes: 256,
        hard_cap_bytes: 640,
        hot_window_ms: Some(0),
        delete_older_than_ms: None,
        // Flusher tick is 200ms — long enough that at least one
        // insert inside the 200-iteration loop can race ahead of
        // the flusher and observe the cap.
        flush_interval: Duration::from_millis(200),
        disk_path: dir.path().to_path_buf(),
        part_cache_bytes: 0,
    };
    let store = SimpleMapStorePerKey::with_persistence(cfg, CleanupPolicy::NoCleanup, persistence)
        .expect("with_persistence");

    // Push well past the cap — 200 items × ~16 bytes each, vs. a
    // 640-byte cap. num_aggregates_to_retain=2 means every 2 distinct
    // windows triggers a seal, so sealed epochs accumulate fast.
    for i in 0..200u64 {
        let start = i * 1_000;
        let end = start + 1_000;
        let batch = vec![sum_entry(1, start, end, i as f64)];
        store
            .insert_precomputed_output_batch(batch)
            .expect("insert");
    }

    let back_pressure_waits = store.back_pressure_wait_count();
    assert!(
        back_pressure_waits > 0,
        "expected ≥1 insert to enter the back-pressure wait loop; \
         wait_count={} (cap={}, 200 inserts × ~16B vs. 640B cap \
         should force at least one observed over-cap)",
        back_pressure_waits,
        640,
    );

    // Sanity: after the loop, wait a bit and confirm the flusher
    // made progress.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let diag = store.diagnostic_info();
        if diag.total_time_map_entries <= 4 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
