//! The data plane's own measurements, for profiling and benchmarks: thread
//! CPU time and wall time of each phase (Remote Write decode, append,
//! precompute per refresh group, query run per query text), query latency,
//! the watermark, bytes held, and each thread's CPU time by name. The data
//! plane renders them, with the stores' and the engine's gauges, on
//! `GET /metrics` in the Prometheus text format.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

/// CPU time this thread has used, in nanoseconds.
pub fn thread_cpu_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid timespec to write.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
    if status != 0 {
        return 0;
    }
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

/// The start of one measured phase on the current thread.
pub struct Timer {
    cpu_ns: u64,
    wall: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            cpu_ns: thread_cpu_ns(),
            wall: Instant::now(),
        }
    }
}

/// How often a phase ran and the thread CPU and wall time it took.
#[derive(Default)]
pub struct PhaseStats {
    runs: AtomicU64,
    cpu_ns: AtomicU64,
    wall_ns: AtomicU64,
}

impl PhaseStats {
    /// Records one run started at `timer`, ending now on the same thread;
    /// returns its wall time in seconds.
    pub fn record(&self, timer: Timer) -> f64 {
        let wall = timer.wall.elapsed();
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.cpu_ns.fetch_add(
            thread_cpu_ns().saturating_sub(timer.cpu_ns),
            Ordering::Relaxed,
        );
        self.wall_ns
            .fetch_add(wall.as_nanos() as u64, Ordering::Relaxed);
        wall.as_secs_f64()
    }

    /// Its runs, CPU and wall time in the Prometheus text format.
    pub fn render(&self, out: &mut String, labels: &str) {
        let seconds = |ns: &AtomicU64| ns.load(Ordering::Relaxed) as f64 / 1e9;
        let _ = writeln!(
            out,
            "asap_phase_runs_total{{{labels}}} {}",
            self.runs.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "asap_phase_cpu_seconds_total{{{labels}}} {}",
            seconds(&self.cpu_ns)
        );
        let _ = writeln!(
            out,
            "asap_phase_wall_seconds_total{{{labels}}} {}",
            seconds(&self.wall_ns)
        );
    }
}

/// Upper bounds of the query latency histogram's buckets, in seconds.
const LATENCY_BUCKETS_S: [f64; 10] = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0];

/// A histogram of query latencies.
#[derive(Default)]
pub struct Latency {
    /// Observations at or below each bucket's bound (not cumulative).
    buckets: [AtomicU64; LATENCY_BUCKETS_S.len()],
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl Latency {
    pub fn observe(&self, seconds: f64) {
        if let Some(bucket) = LATENCY_BUCKETS_S.iter().position(|b| seconds <= *b) {
            self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns
            .fetch_add((seconds * 1e9) as u64, Ordering::Relaxed);
    }

    /// The histogram `name` in the Prometheus text format.
    pub fn render(&self, out: &mut String, name: &str, labels: &str) {
        let mut cumulative = 0;
        for (bound, bucket) in LATENCY_BUCKETS_S.iter().zip(&self.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{labels},le=\"+Inf\"}} {count}");
        let _ = writeln!(
            out,
            "{name}_sum{{{labels}}} {}",
            self.sum_ns.load(Ordering::Relaxed) as f64 / 1e9
        );
        let _ = writeln!(out, "{name}_count{{{labels}}} {count}");
    }
}

/// One refresh group's windows on their way through the precompute task
/// pool.
#[derive(Default)]
pub struct WindowLatency {
    /// Windows committed (refreshed, already stored or failed).
    pub windows: AtomicU64,
    /// From the window closing to its computation starting: the backlog,
    /// waiting for the windows it reads and for a free worker.
    pub queue_wait_ns: AtomicU64,
    /// From its computation ending to its commit: waiting for the group's
    /// earlier windows.
    pub commit_wait_ns: AtomicU64,
    /// From the window's end (event time) to its commit (wall clock).
    pub ready_ms: AtomicU64,
    pub ready_max_ms: AtomicU64,
    /// The runs a window is split into, by series (1: one run).
    pub parts: AtomicU64,
}

impl WindowLatency {
    pub fn record(
        &self,
        queue_wait: std::time::Duration,
        commit_wait: std::time::Duration,
        ready_ms: i64,
    ) {
        self.windows.fetch_add(1, Ordering::Relaxed);
        self.queue_wait_ns
            .fetch_add(queue_wait.as_nanos() as u64, Ordering::Relaxed);
        self.commit_wait_ns
            .fetch_add(commit_wait.as_nanos() as u64, Ordering::Relaxed);
        self.ready_ms.fetch_add(ready_ms as u64, Ordering::Relaxed);
        self.ready_max_ms
            .fetch_max(ready_ms as u64, Ordering::Relaxed);
    }
}

/// One planned query's measurements.
#[derive(Default)]
pub struct QueryMetrics {
    /// The query run of an instant query, on its worker thread.
    pub run: PhaseStats,
    /// From an instant request reaching the engine to its answer, queueing
    /// for a worker included.
    pub latency: Latency,
    /// The query run of one step of a range query.
    pub range_step_run: PhaseStats,
    /// From a range request reaching the data plane to its answer.
    pub range_latency: Latency,
    /// Steps of the range requests answered.
    pub range_steps: AtomicU64,
    /// Of those, the steps the result cache answered.
    pub range_steps_cached: AtomicU64,
}

/// Push precompute's measurements, by refresh group and ingest shard.
pub struct PushMetrics {
    /// Folding one morsel into a group's open windows on a shard.
    pub push: Vec<Vec<PhaseStats>>,
    /// Sealing a window of a group on a shard.
    pub seal: Vec<Vec<PhaseStats>>,
    /// Combining a window's partials, by group.
    pub combine: Vec<PhaseStats>,
    /// Committing pushed windows.
    pub commit: PhaseStats,
    /// Bytes of open-window state, by group and shard.
    pub state_bytes: Vec<Vec<AtomicU64>>,
    /// Open instances by shard.
    pub open_instances: Vec<AtomicU64>,
    /// By shard: some group has more than one window open there.
    pub older_window_open: Vec<AtomicBool>,
    /// From the newest committed window's end to its commit.
    pub commit_lag_ms: AtomicI64,
}

impl PushMetrics {
    pub fn new(groups: usize, shards: usize) -> Self {
        let grid = || {
            (0..groups)
                .map(|_| (0..shards).map(|_| PhaseStats::default()).collect())
                .collect()
        };
        Self {
            push: grid(),
            seal: grid(),
            combine: (0..groups).map(|_| PhaseStats::default()).collect(),
            commit: PhaseStats::default(),
            state_bytes: (0..groups)
                .map(|_| (0..shards).map(|_| AtomicU64::new(0)).collect())
                .collect(),
            open_instances: (0..shards).map(|_| AtomicU64::new(0)).collect(),
            older_window_open: (0..shards).map(|_| AtomicBool::new(false)).collect(),
            commit_lag_ms: AtomicI64::new(0),
        }
    }

    /// Open-window state over every group and shard.
    pub fn total_state_bytes(&self) -> u64 {
        self.state_bytes
            .iter()
            .flatten()
            .map(|b| b.load(Ordering::Relaxed))
            .sum()
    }

    /// Its phases in the Prometheus text format.
    pub fn render(&self, out: &mut String) {
        for (group, shards) in self.push.iter().enumerate() {
            for (shard, stats) in shards.iter().enumerate() {
                stats.render(
                    out,
                    &format!("phase=\"precompute_push\",group=\"{group}\",shard=\"{shard}\""),
                );
            }
        }
        for (group, shards) in self.seal.iter().enumerate() {
            for (shard, stats) in shards.iter().enumerate() {
                stats.render(
                    out,
                    &format!("phase=\"precompute_seal\",group=\"{group}\",shard=\"{shard}\""),
                );
            }
        }
        for (group, stats) in self.combine.iter().enumerate() {
            stats.render(
                out,
                &format!("phase=\"precompute_combine\",group=\"{group}\""),
            );
        }
        self.commit.render(out, "phase=\"precompute_commit\"");
    }

    /// Its gauges in the Prometheus text format.
    pub fn render_gauges(&self, out: &mut String) {
        let _ = writeln!(out, "# HELP asap_precompute_state_bytes Bytes of open push-window state.\n# TYPE asap_precompute_state_bytes gauge");
        for (group, shards) in self.state_bytes.iter().enumerate() {
            for (shard, bytes) in shards.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "asap_precompute_state_bytes{{group=\"{group}\",shard=\"{shard}\"}} {}",
                    bytes.load(Ordering::Relaxed)
                );
            }
        }
        let _ = writeln!(out, "# HELP asap_precompute_open_instances Open push windows per shard.\n# TYPE asap_precompute_open_instances gauge");
        for (shard, open) in self.open_instances.iter().enumerate() {
            let _ = writeln!(
                out,
                "asap_precompute_open_instances{{shard=\"{shard}\"}} {}",
                open.load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            out,
            "# HELP asap_window_commit_lag_seconds From the newest committed window's end to its commit.\n# TYPE asap_window_commit_lag_seconds gauge\nasap_window_commit_lag_seconds {}",
            self.commit_lag_ms.load(Ordering::Relaxed) as f64 / 1e3
        );
    }
}

/// Every measurement the engine's threads record.
pub struct Metrics {
    /// Push precompute's measurements (push mode only).
    pub push: Option<PushMetrics>,
    /// Remote Write decode and series interning, on the HTTP threads.
    pub remote_write_decode: PhaseStats,
    /// Storing one ingest command's samples on the ingest thread: routing,
    /// appending, closing windows, compacting and evicting panes.
    pub append: PhaseStats,
    /// Each refresh group's ingestion runs, with its labels.
    pub precompute: Vec<(String, PhaseStats)>,
    /// Each refresh group's waits around its ingestion runs.
    pub precompute_windows: Vec<WindowLatency>,
    /// Wall time the precompute threads spent computing windows.
    pub precompute_busy_ns: AtomicU64,
    /// Threads computing windows (`--precompute-threads`).
    pub precompute_threads: AtomicU64,
    /// Each planned query's runs, by query text.
    pub queries: BTreeMap<String, QueryMetrics>,
    /// The watermark, `i64::MIN` before the first.
    pub watermark_ms: AtomicI64,
    pub raw_data_bytes: AtomicU64,
    pub raw_data_samples: AtomicU64,
    pub raw_data_panes: AtomicU64,
    pub view_bytes: AtomicU64,
    /// Bytes written to the view files.
    pub view_persisted_bytes: AtomicU64,
    /// Time spent syncing the view files.
    pub view_sync_ns: AtomicU64,
    /// View windows not written to the files (a sketch state, or an error).
    pub view_windows_not_persisted: AtomicU64,
    /// View windows loaded from the files at startup.
    pub view_windows_loaded: AtomicU64,
    /// Time the precompute thread waited for room in the persist backlog.
    pub view_persist_over_bound: AtomicU64,
    /// Bytes of the view files (segments) held.
    pub view_file_bytes: AtomicU64,
    /// View segments whose pages were advised cold.
    pub view_segments_marked_cold: AtomicU64,
}

impl Metrics {
    /// Measurements for refresh groups described by `groups` (window width
    /// and view ids) and the planned `queries`.
    pub fn new(groups: &[(i64, Vec<String>)], queries: &[String]) -> Self {
        Self {
            push: None,
            remote_write_decode: PhaseStats::default(),
            append: PhaseStats::default(),
            precompute: groups
                .iter()
                .enumerate()
                .map(|(index, (width_ms, views))| {
                    let labels = format!(
                        "phase=\"precompute\",group=\"{index}\",window_ms=\"{width_ms}\",views=\"{}\"",
                        views.join(",")
                    );
                    (labels, PhaseStats::default())
                })
                .collect(),
            precompute_windows: groups.iter().map(|_| WindowLatency::default()).collect(),
            precompute_busy_ns: AtomicU64::new(0),
            precompute_threads: AtomicU64::new(1),
            queries: queries
                .iter()
                .map(|query| (query.trim().to_string(), QueryMetrics::default()))
                .collect(),
            watermark_ms: AtomicI64::new(i64::MIN),
            raw_data_bytes: AtomicU64::new(0),
            raw_data_samples: AtomicU64::new(0),
            raw_data_panes: AtomicU64::new(0),
            view_bytes: AtomicU64::new(0),
            view_persisted_bytes: AtomicU64::new(0),
            view_sync_ns: AtomicU64::new(0),
            view_windows_not_persisted: AtomicU64::new(0),
            view_windows_loaded: AtomicU64::new(0),
            view_persist_over_bound: AtomicU64::new(0),
            view_file_bytes: AtomicU64::new(0),
            view_segments_marked_cold: AtomicU64::new(0),
        }
    }

    /// The query's measurements, if it is planned.
    pub fn query(&self, query: &str) -> Option<&QueryMetrics> {
        self.queries.get(query.trim())
    }
}

/// CPU seconds of each live thread, summed by name with trailing digits
/// removed (`query-0`, `query-1`, … are `query-`).
pub fn thread_cpu_seconds() -> BTreeMap<String, f64> {
    // SAFETY: sysconf has no preconditions.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let mut by_name = BTreeMap::new();
    for task in std::fs::read_dir("/proc/self/task").into_iter().flatten() {
        let Ok(task) = task else { continue };
        let path = task.path();
        let (Ok(comm), Ok(stat)) = (
            std::fs::read_to_string(path.join("comm")),
            std::fs::read_to_string(path.join("stat")),
        ) else {
            continue;
        };
        // After the parenthesized name: utime and stime are fields 14 and 15.
        let Some(fields) = stat.rfind(')').map(|end| &stat[end + 2..]) else {
            continue;
        };
        let fields: Vec<&str> = fields.split(' ').collect();
        let (Some(Ok(user)), Some(Ok(system))) = (
            fields.get(11).map(|f| f.parse::<f64>()),
            fields.get(12).map(|f| f.parse::<f64>()),
        ) else {
            continue;
        };
        let name = comm.trim().trim_end_matches(|c: char| c.is_ascii_digit());
        *by_name.entry(name.to_string()).or_insert(0.0) += (user + system) / ticks;
    }
    by_name
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A phase records the thread CPU it burns.
    #[test]
    fn a_phase_records_its_thread_cpu() {
        let stats = PhaseStats::default();
        let timer = Timer::start();
        let mut x = 0u64;
        for i in 0..5_000_000u64 {
            x = x.wrapping_add(i * i);
        }
        std::hint::black_box(x);
        stats.record(timer);
        assert!(stats.cpu_ns.load(Ordering::Relaxed) > 0);
    }
}
