//! Cold view reads, `mmap` versus `pread` (docs/design/tiered-view-storage.md,
//! B3): writes `WINDOWS` windows of `SERIES` rows of one view (states of
//! `SAMPLES` samples each of `FAMILY`, `kll` (default), `ddsketch` (alpha
//! 0.02) or `counter` (exact); a series identity and a value) to a
//! segment, timing the writes, seals it, then decodes every window both
//! ways, with the page cache warm and after dropping it, and prints ns per
//! window and per row, and the process's `RssAnon`/`RssFile`.
//!
//! `cargo run --release -p data_plane --example view_read_bench -- [SERIES] [WINDOWS] [SAMPLES] [DIR] [FAMILY]`

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_executor::planner::ir::schema::{
    DataType, Field, FieldDataType, Schema, PROMQL_SERIES_IDENTITY,
};
use asap_executor::planner::summary_type::{ExactKind, ExactParams, SketchAlgorithm};
use asap_executor::summary_kernels::exact::ExactAccumulator;
use asap_executor::summary_kernels::{DDSketchAccumulator, DatasketchesKLLAccumulator};
use asap_executor::values::{Batch, Value};
use asap_executor::AggregateCore;
use storage::persist::{ViewFiles, ViewRead};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let series: usize = args.get(1).map_or(10_000, |a| a.parse().unwrap());
    let windows: i64 = args.get(2).map_or(40, |a| a.parse().unwrap());
    let samples: usize = args.get(3).map_or(15, |a| a.parse().unwrap());
    let dir = PathBuf::from(args.get(4).map_or("view-read-bench", String::as_str));
    let family_name = args.get(5).map_or("kll", String::as_str);
    let _ = std::fs::remove_dir_all(&dir);

    let family = match family_name {
        "kll" => FieldDataType::Sketch(SketchAlgorithm::Kll),
        "ddsketch" => FieldDataType::Sketch(SketchAlgorithm::DDSketch),
        "counter" => FieldDataType::ExactAggregate(ExactKind::Counter, ExactParams::Counter),
        other => panic!("unknown family {other}: kll, ddsketch or counter"),
    };
    let schema = Arc::new(Schema::with_time_index(
        vec![
            Field::plain("ts", DataType::Timestamp, false),
            Field::plain(PROMQL_SERIES_IDENTITY, DataType::Utf8, false),
            Field::plain("v", DataType::Float64, false),
            Field::new("state", family.clone(), false),
        ],
        0,
        vec![],
    ));
    let mut files = ViewFiles::open(&dir, 1, Duration::from_secs(5)).unwrap();
    let mut locations = Vec::new();
    let mut write = Duration::ZERO;
    for window in 0..windows {
        let end = (window + 1) * 15_000;
        let rows = (0..series)
            .map(|s| {
                let sample = |i: usize| ((s * 31 + i * 7 + window as usize) % 1000) as f64;
                let state: Arc<dyn AggregateCore> = match family_name {
                    "kll" => {
                        let mut kll = DatasketchesKLLAccumulator::new(132);
                        (0..samples).for_each(|i| kll.update(sample(i)));
                        Arc::new(kll)
                    }
                    "ddsketch" => {
                        let mut dd = asap_sketchlib::DdSketch::new(0.02);
                        (0..samples).for_each(|i| dd.add(&sample(i)));
                        Arc::new(DDSketchAccumulator::restore_from_disk(dd).unwrap())
                    }
                    _ => {
                        let mut counter = ExactAccumulator::new(family.clone(), false).unwrap();
                        (0..samples).for_each(|i| {
                            counter.update(
                                None,
                                (window as usize * samples + i) as f64,
                                end - 15_000 + i as i64 * 100,
                            )
                        });
                        Arc::new(counter)
                    }
                };
                vec![
                    Value::Timestamp(end),
                    Value::Utf8(
                        format!(
                            r#"{{"__name__":"lat","instance":"i{s}","job":"job{}"}}"#,
                            s % 16
                        )
                        .into(),
                    ),
                    Value::Float64(s as f64),
                    Value::Summary {
                        family: family.clone(),
                        state,
                    },
                ]
            })
            .collect();
        let batch = Batch::try_new(schema.clone(), rows).unwrap();
        let started = Instant::now();
        let written = files
            .append(0, (end - 15_000, end), &[(1, std::slice::from_ref(&batch))])
            .unwrap();
        write += started.elapsed();
        let location = written
            .locations
            .into_iter()
            .next()
            .expect("no window location: segments are not implemented")
            .1;
        locations.push(location);
    }
    files.seal(0).unwrap();
    let bytes: u64 = locations.iter().map(|l| u64::from(l.len())).sum();
    println!(
        "{windows} windows x {series} rows, {samples} samples per {family_name} state: {:.1} MiB of fragments, {:.1} B per row; write {:.0} ns per row",
        bytes as f64 / 1048576.0,
        bytes as f64 / (windows as f64 * series as f64),
        write.as_nanos() as f64 / (windows as f64 * series as f64)
    );
    // pread first: a mapped page cannot be dropped from the page cache.
    for how in [ViewRead::Pread, ViewRead::Mmap] {
        for cold in [true, false] {
            if cold {
                drop_page_cache(locations[0].segment_path());
            }
            let started = Instant::now();
            for location in &locations {
                std::hint::black_box(location.read(&schema, how).unwrap().0);
            }
            let ns = started.elapsed().as_nanos() as f64;
            println!(
                "{how:?} {}: {:.2} ms per window, {:.0} ns per row; {}",
                if cold { "cold" } else { "warm" },
                ns / windows as f64 / 1e6,
                ns / (windows as f64 * series as f64),
                rss()
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn drop_page_cache(path: &std::path::Path) {
    if let Ok(file) = std::fs::File::open(path) {
        use std::os::fd::AsRawFd;
        // SAFETY: a valid open descriptor; advice only.
        unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    }
}

fn rss() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .filter(|line| line.starts_with("RssAnon") || line.starts_with("RssFile"))
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join(", ")
}
