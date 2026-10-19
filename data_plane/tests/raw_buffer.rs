//! The local raw buffer holds the recent samples query-time raw windows
//! read, and says from when a read must come from Prometheus instead.

use std::collections::BTreeMap;

use data_plane::precompute::{shard_of, RawSample};
use data_plane::raw_buffer::RawBuffer;

const MINUTE: i64 = 60_000;

/// Samples every 15 s in `[from, to)` of `metric` for each of `series`.
fn samples(metric: &str, series: usize, from: i64, to: i64) -> Vec<RawSample> {
    let labels: Vec<_> = (0..series)
        .map(|i| {
            data_plane::series_ids::intern(&BTreeMap::from([
                ("__name__".to_string(), metric.to_string()),
                ("instance".to_string(), i.to_string()),
            ]))
        })
        .collect();
    labels
        .iter()
        .flat_map(|labels| {
            (from..to).step_by(15_000).map(move |t| RawSample {
                labels: labels.clone(),
                timestamp_ms: t,
                value: t as f64,
            })
        })
        .collect()
}

/// `samples` appended as the ingestion workers would: each series to its
/// shard, in request order.
fn append(buffer: &RawBuffer, shards: usize, samples: &[RawSample]) {
    let mut by_shard = vec![Vec::new(); shards];
    for sample in samples {
        by_shard[shard_of(&sample.labels, shards)].push(sample.clone());
    }
    for (shard, samples) in by_shard.iter().enumerate() {
        buffer.append(shard, samples);
    }
}

fn sorted(mut samples: Vec<RawSample>) -> Vec<(BTreeMap<String, String>, i64)> {
    let mut points: Vec<_> = samples
        .drain(..)
        .map(|s| ((*s.labels).clone(), s.timestamp_ms))
        .collect();
    points.sort();
    points
}

/// A read inside the horizon is the buffer's alone and equals the samples
/// of the metric in `(start, end]`; another metric's are not returned.
#[test]
fn reads_recent_samples_of_one_metric() {
    let buffer = RawBuffer::new(10 * MINUTE, 3);
    let all = samples("m", 5, 0, 20 * MINUTE + 1);
    append(&buffer, 3, &all);
    append(&buffer, 3, &samples("other", 2, 0, 20 * MINUTE + 1));
    let read = buffer.read("m", 15 * MINUTE, 20 * MINUTE);
    assert_eq!(read.older_than_ms, 15 * MINUTE, "nothing from Prometheus");
    let expected: Vec<_> = all
        .iter()
        .filter(|s| 15 * MINUTE < s.timestamp_ms && s.timestamp_ms <= 20 * MINUTE)
        .cloned()
        .collect();
    assert_eq!(sorted(read.samples), sorted(expected));
}

/// The buffer keeps its horizon back from the newest sample: a read that
/// starts earlier gets the samples after the cut from the buffer, and must
/// read the rest, up to the cut, from Prometheus.
#[test]
fn reads_past_the_horizon_split_at_the_cut() {
    let buffer = RawBuffer::new(5 * MINUTE, 2);
    let all = samples("m", 4, 0, 20 * MINUTE + 1);
    append(&buffer, 2, &all);
    let read = buffer.read("m", 10 * MINUTE, 20 * MINUTE);
    assert_eq!(read.older_than_ms, 15 * MINUTE);
    assert!(read.samples.iter().all(|s| s.timestamp_ms > 15 * MINUTE));
    assert_eq!(read.samples.len(), 4 * 20);
    let (series, held) = buffer.size();
    assert_eq!((series, held), (4, 4 * 20), "older samples are dropped");
}

/// Before its first sample the buffer holds nothing: a read of earlier
/// times comes from Prometheus up to that sample.
#[test]
fn samples_before_startup_come_from_prometheus() {
    let buffer = RawBuffer::new(30 * MINUTE, 1);
    assert_eq!(buffer.read("m", 0, MINUTE).older_than_ms, MINUTE);
    append(
        &buffer,
        1,
        &samples("m", 1, 4 * MINUTE + 30_000, 5 * MINUTE + 1),
    );
    let read = buffer.read("m", 4 * MINUTE, 5 * MINUTE);
    assert_eq!(read.older_than_ms, 4 * MINUTE + 30_000);
    assert_eq!(read.samples.len(), 2, "4:45 and 5:00");
}

/// Each metric is kept for its own longest read, and a metric no raw
/// window reads is not kept at all.
#[test]
fn keeps_each_metric_for_its_own_horizon() {
    let horizons = BTreeMap::from([
        ("short".to_string(), MINUTE),
        ("long".to_string(), 10 * MINUTE),
    ]);
    let buffer = RawBuffer::with_horizons(horizons, 2);
    for metric in ["short", "long", "unread"] {
        append(&buffer, 2, &samples(metric, 3, 0, 20 * MINUTE + 1));
    }
    assert_eq!(
        buffer.read("short", 0, 20 * MINUTE).older_than_ms,
        19 * MINUTE
    );
    assert_eq!(
        buffer.read("long", 0, 20 * MINUTE).older_than_ms,
        10 * MINUTE
    );
    assert_eq!(
        buffer.size(),
        (6, 3 * 4 + 3 * 40),
        "unread samples are not kept"
    );
}

/// The plan's horizons: Example 1 reads its counter at query time over
/// 1-minute windows, nothing else.
#[test]
fn horizons_come_from_the_plans_query_time_reads() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../examples/planner-layering-example1.json");
    let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let plan = control_plane::plan_workload(&workload, &control_plane::DeploymentInputs::default())
        .unwrap();
    assert_eq!(
        RawBuffer::horizons(&plan),
        BTreeMap::from([("http_requests_total".to_string(), MINUTE)])
    );
}
