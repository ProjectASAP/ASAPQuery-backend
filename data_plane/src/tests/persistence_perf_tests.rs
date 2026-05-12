//! Performance harness for the `SketchStore` persistence layer.
//!
//! All tests are `#[ignore]` so they don't slow down the normal
//! `cargo test` run. Exercise them with:
//!
//! ```text
//! cargo test --release -p data_plane --lib \
//!     tests::persistence_perf_tests -- --ignored --nocapture
//! ```
//!
//! Numbers are illustrative, not load-bearing — ext4 on a laptop SSD
//! is nowhere near the profile of a production deployment, but the
//! *shapes* (memory-bound adherence, relative overheads, disk read
//! costs vs. in-memory) are informative for tuning the flusher.
//!
//! ## Scenarios
//!
//! 1. [`insert_throughput_in_memory_vs_persistent`] — raw insert
//!    throughput with and without persistence enabled. Measures the
//!    flusher's overhead on the write path.
//! 2. [`query_latency_memory_only_vs_disk_through`] — p50 / p99
//!    range-query latency when results come from memory only vs.
//!    when everything has been flushed to disk.
//! 3. [`flush_throughput_sustained`] — how fast the background
//!    flusher can turn sealed epochs into parts and evict them, in
//!    entries/sec and MiB/sec.
//! 4. [`memory_bound_adherence_under_overload`] — verifies the
//!    `mem_bytes_sealed` counter stays bounded when the insert rate
//!    vastly exceeds the flusher's steady-state throughput.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use promql_utilities::data_model::KeyByLabelNames;
use tempfile::TempDir;

use crate::stores::types::{
    AggregationType, CleanupPolicy, PrecomputedOutput, StreamingConfig, WindowType,
};
use crate::precompute_engine::operators::SumAccumulator;
use crate::stores::sketch_db::store::per_key::SketchStorePerKey;
use crate::stores::sketch_db::store::persistence::SketchStorePersistenceConfig;
use crate::stores::Store;
use crate::{AggregateCore, AggregationConfig};

// =========================================================================
// Fixtures
// =========================================================================

fn streaming_config(agg_id: u64, retention: Option<u64>) -> Arc<StreamingConfig> {
    let cfg = AggregationConfig::new(
        agg_id,
        AggregationType::Sum,
        String::new(),
        HashMap::new(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        60,
        60,
        WindowType::Tumbling,
        String::new(),
        "cpu_usage".to_string(),
        retention,
        None,
        None,
        None,
    );
    let mut map = HashMap::new();
    map.insert(agg_id, cfg);
    Arc::new(StreamingConfig::new(map))
}

fn persistence_cfg(
    dir: &TempDir,
    memory_limit_bytes: usize,
    hot_window_ms: Option<u64>,
    flush_interval: Duration,
) -> SketchStorePersistenceConfig {
    SketchStorePersistenceConfig {
        memory_limit_bytes,
        memory_low_watermark_bytes: memory_limit_bytes * 8 / 10,
        hard_cap_bytes: memory_limit_bytes * 125 / 100,
        hot_window_ms,
        delete_older_than_ms: None,
        flush_interval,
        disk_path: dir.path().to_path_buf(),
        part_cache_bytes: 64 * 1024 * 1024,
    }
}

fn sum_item(
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

/// Generate N distinct-window items. Each has `start_ts = i * 1000`
/// so the rotator fires regularly.
fn gen_items(agg_id: u64, n: usize) -> Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let start = (i as u64) * 1000;
        let end = start + 1000;
        out.push(sum_item(agg_id, start, end, i as f64));
    }
    out
}

/// Insert items in batches of `batch_size`, returning wall-clock time.
fn insert_all<S: Store>(
    store: &S,
    items: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    batch_size: usize,
) -> Duration {
    let start = Instant::now();
    for chunk in items.chunks(batch_size) {
        let batch: Vec<_> = chunk.iter().map(|(o, a)| (o.clone(), a.clone())).collect();
        store.insert_precomputed_output_batch(batch).unwrap();
    }
    start.elapsed()
}

fn fmt_rate(count: usize, d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs == 0.0 {
        return "∞".to_string();
    }
    let rate = count as f64 / secs;
    if rate >= 1_000_000.0 {
        format!("{:.2} M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1} K/s", rate / 1_000.0)
    } else {
        format!("{:.0}/s", rate)
    }
}

// =========================================================================
// Scenario 1: insert throughput, memory-only vs. with-persistence
// =========================================================================

#[test]
#[ignore]
fn insert_throughput_in_memory_vs_persistent() {
    const N: usize = 200_000;
    const BATCH: usize = 1_000;

    println!("\n== Insert throughput ({} items, batch={}) ==", N, BATCH);

    // -- baseline: in-memory, NoCleanup --
    {
        let store = SketchStorePerKey::new(streaming_config(1, None), CleanupPolicy::NoCleanup);
        let items = gen_items(1, N);
        let d = insert_all(&store, items, BATCH);
        println!(
            "  in-memory (NoCleanup):         {} total, {:?} wall, {} inserts/sec",
            N,
            d,
            fmt_rate(N, d)
        );
    }

    // -- with persistence, flusher idle (no flush, just the overhead
    // of mem_bytes_sealed tracking + always-rotate) --
    {
        let tmp = TempDir::new().unwrap();
        // Large memory limit, no hot window — flusher has nothing to
        // do, so this isolates the insert-path overhead of persistence
        // being enabled.
        let cfg = persistence_cfg(
            &tmp,
            16 * 1024 * 1024 * 1024, // 16 GiB ceiling — unreachable in this test
            None,
            Duration::from_secs(3600),
        );
        let store = SketchStorePerKey::with_persistence(
            streaming_config(1, Some(1024)),
            CleanupPolicy::NoCleanup,
            cfg,
        )
        .unwrap();
        let items = gen_items(1, N);
        let d = insert_all(&store, items, BATCH);
        println!(
            "  persistent (flusher idle):     {} total, {:?} wall, {} inserts/sec",
            N,
            d,
            fmt_rate(N, d)
        );
    }

    // -- with persistence + aggressive flushing --
    {
        let tmp = TempDir::new().unwrap();
        let cfg = persistence_cfg(
            &tmp,
            4 * 1024 * 1024, // 4 MiB limit — forces constant pressure
            Some(0),         // flush everything ASAP
            Duration::from_millis(25),
        );
        let store = SketchStorePerKey::with_persistence(
            streaming_config(1, Some(512)),
            CleanupPolicy::NoCleanup,
            cfg,
        )
        .unwrap();
        let items = gen_items(1, N);
        let d = insert_all(&store, items, BATCH);
        println!(
            "  persistent (flusher active):   {} total, {:?} wall, {} inserts/sec",
            N,
            d,
            fmt_rate(N, d)
        );
    }
}

// =========================================================================
// Scenario 2: query latency memory-only vs. disk-through
// =========================================================================

#[test]
#[ignore]
fn query_latency_memory_only_vs_disk_through() {
    const POPULATE: usize = 20_000;
    const QUERIES: usize = 1_000;

    println!(
        "\n== Query latency ({} items populated, {} queries) ==",
        POPULATE, QUERIES
    );

    // -- in-memory baseline --
    {
        let store = SketchStorePerKey::new(streaming_config(1, None), CleanupPolicy::NoCleanup);
        let items = gen_items(1, POPULATE);
        insert_all(&store, items, 1_000);

        let lats = run_queries(&store, QUERIES, POPULATE);
        report_latencies("  in-memory", &lats);
    }

    // -- disk read-through (everything flushed) --
    {
        let tmp = TempDir::new().unwrap();
        let cfg = persistence_cfg(&tmp, 4 * 1024 * 1024, Some(0), Duration::from_millis(10));
        let store = SketchStorePerKey::with_persistence(
            streaming_config(1, Some(256)),
            CleanupPolicy::NoCleanup,
            cfg,
        )
        .unwrap();
        let items = gen_items(1, POPULATE);
        insert_all(&store, items, 1_000);

        // Wait until sealed epochs have been flushed and evicted.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let diag = store.diagnostic_info();
            if diag.total_sketch_bytes == 0 && diag.total_time_map_entries < POPULATE / 20 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let lats = run_queries(&store, QUERIES, POPULATE);
        report_latencies("  disk-through", &lats);
    }
}

fn run_queries<S: Store>(store: &S, n_queries: usize, populated: usize) -> Vec<Duration> {
    let mut lats = Vec::with_capacity(n_queries);
    // Deterministic range spread: each query covers ~10% of the window.
    let window = (populated as u64) * 1000;
    let step = window / n_queries as u64;
    let span = window / 10;
    for i in 0..n_queries {
        let start = (i as u64) * step;
        let end = start + span;
        let t = Instant::now();
        let _ = store
            .query_precomputed_output("cpu_usage", 1, start, end)
            .unwrap();
        lats.push(t.elapsed());
    }
    lats
}

fn report_latencies(label: &str, lats: &[Duration]) {
    let mut sorted: Vec<u128> = lats.iter().map(|d| d.as_micros()).collect();
    sorted.sort_unstable();
    let p = |frac: f64| -> u128 {
        let idx = ((sorted.len() as f64 - 1.0) * frac) as usize;
        sorted[idx]
    };
    let avg: u128 = sorted.iter().sum::<u128>() / sorted.len() as u128;
    println!(
        "{}: p50={}µs  p90={}µs  p99={}µs  max={}µs  avg={}µs  n={}",
        label,
        p(0.50),
        p(0.90),
        p(0.99),
        sorted.last().copied().unwrap_or(0),
        avg,
        sorted.len()
    );
}

// =========================================================================
// Scenario 3: sustained flush throughput
// =========================================================================

#[test]
#[ignore]
fn flush_throughput_sustained() {
    const N: usize = 100_000;

    println!("\n== Flush throughput (N={}) ==", N);

    let tmp = TempDir::new().unwrap();
    // Generous memory limit so the flusher is driven by hot_window,
    // not by pressure — this measures the flusher's steady-state,
    // not its back-pressure behavior.
    let cfg = persistence_cfg(
        &tmp,
        128 * 1024 * 1024,
        Some(0), // flush as fast as sealed epochs arrive
        Duration::from_millis(10),
    );
    let store = SketchStorePerKey::with_persistence(
        streaming_config(1, Some(512)),
        CleanupPolicy::NoCleanup,
        cfg,
    )
    .unwrap();

    let items = gen_items(1, N);
    let insert_start = Instant::now();
    insert_all(&store, items, 1_000);
    let insert_d = insert_start.elapsed();

    // Wait for the flusher to drain sealed epochs. We can't wait for
    // `total_sketch_bytes == 0` because the (unsealed) current epoch
    // contributes to that counter and it's never flushed while hot.
    // Instead, wait until the in-memory count of time-map entries
    // drops to ≤ one epoch's capacity — i.e., everything that CAN be
    // flushed has been flushed.
    //
    // "One epoch's worth" = 512 (the retention configured above).
    let drain_start = Instant::now();
    let deadline = drain_start + Duration::from_secs(20);
    let epoch_capacity_hint = 512usize;
    loop {
        let diag = store.diagnostic_info();
        if diag.total_time_map_entries <= epoch_capacity_hint {
            break;
        }
        if Instant::now() >= deadline {
            println!(
                "  WARNING: flusher didn't drain in time; {} time-map entries still in memory",
                diag.total_time_map_entries
            );
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let drain_d = drain_start.elapsed();
    let flushed = N.saturating_sub(epoch_capacity_hint);

    // Measure on-disk bytes.
    let parts_dir = tmp.path().join("parts");
    let total_bytes = dir_size(&parts_dir);

    println!(
        "  insert: {:?} ({} items/s)",
        insert_d,
        fmt_rate(N, insert_d)
    );
    println!(
        "  drain:  {:?} ({} items flushed, {} items/s)",
        drain_d,
        flushed,
        fmt_rate(flushed, drain_d)
    );
    println!(
        "  on-disk: {:.2} MiB ({:.2} MiB/s while draining)",
        total_bytes as f64 / (1024.0 * 1024.0),
        (total_bytes as f64 / (1024.0 * 1024.0)) / drain_d.as_secs_f64()
    );
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if !path.exists() {
        return 0;
    }
    let walk = |p: &std::path::Path, out: &mut u64| {
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let meta = match e.metadata() {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if meta.is_file() {
                    *out += meta.len();
                } else if meta.is_dir() {
                    if let Ok(inner) = std::fs::read_dir(e.path()) {
                        for inner_e in inner.flatten() {
                            if let Ok(m) = inner_e.metadata() {
                                if m.is_file() {
                                    *out += m.len();
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    walk(path, &mut total);
    total
}

// =========================================================================
// Scenario 4: memory-bound adherence under overload
// =========================================================================

#[test]
#[ignore]
fn memory_bound_adherence_under_overload() {
    // Push 10x more than the high-water mark; verify the tracked
    // memory counter stays close to it throughout.
    const N: usize = 50_000;
    const LIMIT_BYTES: usize = 256 * 1024;

    println!(
        "\n== Memory-bound adherence (N={}, limit={} KiB) ==",
        N,
        LIMIT_BYTES / 1024
    );

    let tmp = TempDir::new().unwrap();
    let cfg = persistence_cfg(&tmp, LIMIT_BYTES, Some(0), Duration::from_millis(10));
    let store = SketchStorePerKey::with_persistence(
        streaming_config(1, Some(128)),
        CleanupPolicy::NoCleanup,
        cfg,
    )
    .unwrap();

    let items = gen_items(1, N);

    let mut peak_tracked: usize = 0;
    let start = Instant::now();
    for chunk in items.chunks(500) {
        let batch: Vec<_> = chunk.iter().map(|(o, a)| (o.clone(), a.clone())).collect();
        store.insert_precomputed_output_batch(batch).unwrap();
        let diag = store.diagnostic_info();
        peak_tracked = peak_tracked.max(diag.total_sketch_bytes);
    }
    let d = start.elapsed();

    // Let the flusher drain everything.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if store.diagnostic_info().total_sketch_bytes == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let final_tracked = store.diagnostic_info().total_sketch_bytes;

    println!("  inserted {} items in {:?} ({})", N, d, fmt_rate(N, d));
    println!(
        "  peak tracked: {} KiB  (high-water = {} KiB, ratio = {:.2}x)",
        peak_tracked / 1024,
        LIMIT_BYTES / 1024,
        peak_tracked as f64 / LIMIT_BYTES as f64
    );
    println!("  final tracked after drain: {} bytes", final_tracked);
}
