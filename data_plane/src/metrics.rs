//! The data plane's own measurements, for profiling and benchmarks,
//! exported on `GET /metrics` in the Prometheus text format: thread CPU
//! time and wall time of each phase (Remote Write decode, append,
//! precompute per refresh group, query run per query text), query
//! latency, sample and window counters, backlogs, watermark lag, bytes
//! held, and each thread's CPU time by name.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use crate::ingestion::IngestionStats;
use crate::result_cache::ResultCache;
use crate::store::QueryTimeResults;

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

    fn render(&self, out: &mut String, labels: &str) {
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

    fn render(&self, out: &mut String, name: &str, labels: &str) {
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

/// Every measurement the engine's threads record.
pub struct Metrics {
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
    pub view_persist_wait_ns: AtomicU64,
}

impl Metrics {
    /// Measurements for refresh groups described by `groups` (window width
    /// and view ids) and the planned `queries`.
    pub fn new(groups: &[(i64, Vec<String>)], queries: &[String]) -> Self {
        Self {
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
            view_persist_wait_ns: AtomicU64::new(0),
        }
    }

    /// The query's measurements, if it is planned.
    pub fn query(&self, query: &str) -> Option<&QueryMetrics> {
        self.queries.get(query.trim())
    }
}

/// What `/metrics` reports beside [`Metrics`].
pub struct Gauges<'a> {
    pub stats: IngestionStats,
    pub samples_queued: u64,
    pub precompute_backlog: u64,
    pub persist_backlog: u64,
    pub persist_backlog_bytes: u64,
    pub now_ms: i64,
    pub query_time_results: &'a QueryTimeResults,
    /// `None` when the result cache is off.
    pub result_cache: Option<&'a ResultCache>,
}

/// `metrics` and `gauges` in the Prometheus text format.
pub fn render(metrics: &Metrics, gauges: &Gauges) -> String {
    let mut out = String::new();
    let family = |out: &mut String, name: &str, kind: &str, help: &str| {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
    };

    family(
        &mut out,
        "asap_phase_runs_total",
        "counter",
        "Runs of each measured phase.",
    );
    family(
        &mut out,
        "asap_phase_cpu_seconds_total",
        "counter",
        "Thread CPU time of each measured phase.",
    );
    family(
        &mut out,
        "asap_phase_wall_seconds_total",
        "counter",
        "Wall time of each measured phase.",
    );
    metrics
        .remote_write_decode
        .render(&mut out, "phase=\"remote_write_decode\"");
    metrics.append.render(&mut out, "phase=\"append\"");
    for (labels, stats) in &metrics.precompute {
        stats.render(&mut out, labels);
    }
    for (query, query_metrics) in &metrics.queries {
        let labels = format!("phase=\"query\",query=\"{}\"", escape(query));
        query_metrics.run.render(&mut out, &labels);
        let labels = format!("phase=\"query_range_step\",query=\"{}\"", escape(query));
        query_metrics.range_step_run.render(&mut out, &labels);
    }

    family(
        &mut out,
        "asap_query_latency_seconds",
        "histogram",
        "Planned instant query latency in the engine, queueing for a worker included.",
    );
    for (query, query_metrics) in &metrics.queries {
        let labels = format!("query=\"{}\"", escape(query));
        query_metrics
            .latency
            .render(&mut out, "asap_query_latency_seconds", &labels);
    }
    family(
        &mut out,
        "asap_range_query_latency_seconds",
        "histogram",
        "Planned range query latency, every step included.",
    );
    for (query, query_metrics) in &metrics.queries {
        let labels = format!("query=\"{}\"", escape(query));
        query_metrics
            .range_latency
            .render(&mut out, "asap_range_query_latency_seconds", &labels);
    }
    for (name, help, count) in [
        (
            "asap_range_query_steps_total",
            "Steps of the planned range requests answered.",
            (|m: &QueryMetrics| &m.range_steps) as fn(&QueryMetrics) -> &AtomicU64,
        ),
        (
            "asap_range_query_steps_cached_total",
            "Steps of the planned range requests the result cache answered.",
            |m: &QueryMetrics| &m.range_steps_cached,
        ),
    ] {
        family(&mut out, name, "counter", help);
        for (query, query_metrics) in &metrics.queries {
            let _ = writeln!(
                out,
                "{name}{{query=\"{}\"}} {}",
                escape(query),
                count(query_metrics).load(Ordering::Relaxed)
            );
        }
    }

    family(
        &mut out,
        "asap_samples_total",
        "counter",
        "Samples by what ingestion did with them.",
    );
    let stats = gauges.stats;
    for (outcome, count) in [
        ("accepted", stats.samples_accepted),
        ("dropped_late", stats.samples_dropped_late),
        ("dropped_future", stats.samples_dropped_future),
        ("unrouted", stats.samples_unrouted),
    ] {
        let _ = writeln!(out, "asap_samples_total{{outcome=\"{outcome}\"}} {count}");
    }
    family(
        &mut out,
        "asap_windows_total",
        "counter",
        "Refresh windows by outcome.",
    );
    for (outcome, count) in [
        ("refreshed", stats.windows_refreshed),
        ("failed", stats.windows_failed),
        ("skipped_first", stats.windows_skipped_first),
        ("skipped_expired", stats.windows_skipped_expired),
        ("already_stored", stats.windows_already_stored),
    ] {
        let _ = writeln!(out, "asap_windows_total{{outcome=\"{outcome}\"}} {count}");
    }

    let gauge = |out: &mut String, name: &str, help: &str, value: f64| {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}"
        );
    };
    gauge(
        &mut out,
        "asap_samples_queued",
        "Samples queued for the ingest thread.",
        gauges.samples_queued as f64,
    );
    gauge(
        &mut out,
        "asap_precompute_backlog_windows",
        "Refresh windows closed and not yet refreshed.",
        gauges.precompute_backlog as f64,
    );
    gauge(
        &mut out,
        "asap_persist_backlog_windows",
        "Refreshed windows not yet written to the view files.",
        gauges.persist_backlog as f64,
    );
    gauge(
        &mut out,
        "asap_persist_backlog_bytes",
        "Bytes of refreshed windows queued for the persist threads.",
        gauges.persist_backlog_bytes as f64,
    );
    let watermark_ms = metrics.watermark_ms.load(Ordering::Relaxed);
    if watermark_ms != i64::MIN {
        gauge(
            &mut out,
            "asap_watermark_lag_seconds",
            "Wall clock minus the watermark.",
            (gauges.now_ms - watermark_ms) as f64 / 1e3,
        );
    }
    let load = |value: &AtomicU64| value.load(Ordering::Relaxed) as f64;
    gauge(
        &mut out,
        "asap_raw_data_bytes",
        "Bytes of raw samples held.",
        load(&metrics.raw_data_bytes),
    );
    gauge(
        &mut out,
        "asap_raw_data_samples",
        "Raw samples held.",
        load(&metrics.raw_data_samples),
    );
    gauge(
        &mut out,
        "asap_raw_data_panes",
        "Raw-data panes held, over every metric.",
        load(&metrics.raw_data_panes),
    );
    gauge(
        &mut out,
        "asap_view_bytes",
        "Bytes of materialized view rows held.",
        load(&metrics.view_bytes),
    );
    for (name, help, value) in [
        (
            "asap_view_persisted_bytes_total",
            "Bytes written to the view files.",
            load(&metrics.view_persisted_bytes),
        ),
        (
            "asap_view_sync_seconds_total",
            "Time spent syncing the view files.",
            load(&metrics.view_sync_ns) / 1e9,
        ),
        (
            "asap_view_windows_not_persisted_total",
            "View windows not written to the view files (a sketch state, or an error).",
            load(&metrics.view_windows_not_persisted),
        ),
        (
            "asap_view_persist_wait_seconds_total",
            "Time the precompute thread waited for room in the persist queue.",
            load(&metrics.view_persist_wait_ns) / 1e9,
        ),
        (
            "asap_view_windows_loaded_total",
            "View windows loaded from the view files at startup.",
            load(&metrics.view_windows_loaded),
        ),
    ] {
        family(&mut out, name, "counter", help);
        let _ = writeln!(out, "{name} {value}");
    }
    let seconds = |ns: &AtomicU64| ns.load(Ordering::Relaxed) as f64 / 1e9;
    for (name, help, value) in [
        (
            "asap_precompute_windows_total",
            "Refresh windows committed, by refresh group.",
            (|w: &WindowLatency| w.windows.load(Ordering::Relaxed) as f64)
                as fn(&WindowLatency) -> f64,
        ),
        (
            "asap_precompute_queue_wait_seconds_total",
            "Time from a window closing to its computation starting, by refresh group.",
            |w| w.queue_wait_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ),
        (
            "asap_precompute_commit_wait_seconds_total",
            "Time from a window computed to its commit, waiting for the group's earlier windows.",
            |w| w.commit_wait_ns.load(Ordering::Relaxed) as f64 / 1e9,
        ),
        (
            "asap_window_ready_seconds_total",
            "Time from a window's end (event time) to its commit (wall clock), by refresh group.",
            |w| w.ready_ms.load(Ordering::Relaxed) as f64 / 1e3,
        ),
    ] {
        family(&mut out, name, "counter", help);
        for (group, windows) in metrics.precompute_windows.iter().enumerate() {
            let _ = writeln!(out, "{name}{{group=\"{group}\"}} {}", value(windows));
        }
    }
    family(
        &mut out,
        "asap_window_ready_seconds_max",
        "gauge",
        "The longest time from a window's end to its commit, by refresh group.",
    );
    for (group, windows) in metrics.precompute_windows.iter().enumerate() {
        let _ = writeln!(
            out,
            "asap_window_ready_seconds_max{{group=\"{group}\"}} {}",
            windows.ready_max_ms.load(Ordering::Relaxed) as f64 / 1e3
        );
    }
    family(
        &mut out,
        "asap_precompute_window_parts",
        "gauge",
        "The runs a refresh group's window is split into, by series; 1 when some node combines series.",
    );
    for (group, windows) in metrics.precompute_windows.iter().enumerate() {
        let _ = writeln!(
            out,
            "asap_precompute_window_parts{{group=\"{group}\"}} {}",
            windows.parts.load(Ordering::Relaxed)
        );
    }
    family(
        &mut out,
        "asap_precompute_busy_seconds_total",
        "counter",
        "Wall time the precompute threads spent computing windows; divided by asap_precompute_threads, the task pool's utilisation.",
    );
    let _ = writeln!(
        out,
        "asap_precompute_busy_seconds_total {}",
        seconds(&metrics.precompute_busy_ns)
    );
    gauge(
        &mut out,
        "asap_precompute_threads",
        "Threads computing refresh windows.",
        load(&metrics.precompute_threads),
    );
    let results = gauges.query_time_results;
    gauge(
        &mut out,
        "asap_query_time_result_bytes",
        "Bytes of query-time OneEvaluation results held.",
        results.bytes() as f64,
    );
    gauge(
        &mut out,
        "asap_query_time_results",
        "Query-time OneEvaluation results held, computed or being computed.",
        results.len() as f64,
    );
    family(
        &mut out,
        "asap_query_time_result_computes_total",
        "counter",
        "Query-time OneEvaluation results computed (misses).",
    );
    let _ = writeln!(
        out,
        "asap_query_time_result_computes_total {}",
        results.computes()
    );
    family(
        &mut out,
        "asap_query_time_result_hits_total",
        "counter",
        "Reads of a query-time OneEvaluation result already computed.",
    );
    let _ = writeln!(out, "asap_query_time_result_hits_total {}", results.hits());
    if let Some(cache) = gauges.result_cache {
        gauge(
            &mut out,
            "asap_result_cache_bytes",
            "Bytes of final query answers the result cache keeps (estimated).",
            cache.bytes() as f64,
        );
        gauge(
            &mut out,
            "asap_result_cache_entries",
            "Final query answers the result cache keeps, by query and evaluation time.",
            cache.len() as f64,
        );
        for (name, help, value) in [
            (
                "asap_result_cache_hits_total",
                "Result cache lookups that found the answer.",
                cache.hits(),
            ),
            (
                "asap_result_cache_misses_total",
                "Result cache lookups that found no answer.",
                cache.misses(),
            ),
            (
                "asap_result_cache_evictions_total",
                "Answers the result cache dropped by age or by bytes.",
                cache.evictions(),
            ),
        ] {
            family(&mut out, name, "counter", help);
            let _ = writeln!(out, "{name} {value}");
        }
    }
    if let Some(rss) = resident_bytes() {
        gauge(
            &mut out,
            "process_resident_memory_bytes",
            "Resident memory of the process.",
            rss as f64,
        );
    }

    family(
        &mut out,
        "asap_thread_cpu_seconds_total",
        "counter",
        "CPU time of the live threads, by name (trailing digits removed).",
    );
    for (thread, seconds) in thread_cpu_seconds() {
        let _ = writeln!(
            out,
            "asap_thread_cpu_seconds_total{{thread=\"{}\"}} {seconds}",
            escape(&thread)
        );
    }
    out
}

/// `text` as a label value.
fn escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Resident set size, from `/proc/self/statm`.
fn resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split(' ').nth(1)?.parse().ok()?;
    // SAFETY: sysconf has no preconditions.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    Some(pages * u64::try_from(page_size).ok()?)
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

    /// A phase records the thread CPU it burns, and the text format names
    /// each phase and query.
    #[test]
    fn records_and_renders_phases() {
        let metrics = Metrics::new(
            &[(10_000, vec!["3".into(), "5".into()])],
            &["sum(x)".to_string()],
        );
        let timer = Timer::start();
        let mut x = 0u64;
        for i in 0..5_000_000u64 {
            x = x.wrapping_add(i * i);
        }
        std::hint::black_box(x);
        metrics.append.record(timer);
        assert!(metrics.append.cpu_ns.load(Ordering::Relaxed) > 0);
        metrics.query("sum(x) ").unwrap().latency.observe(0.003);
        let text = render(
            &metrics,
            &Gauges {
                stats: IngestionStats::default(),
                samples_queued: 0,
                precompute_backlog: 2,
                persist_backlog: 0,
                persist_backlog_bytes: 0,
                now_ms: 0,
                query_time_results: &QueryTimeResults::new(0, 0),
                result_cache: None,
            },
        );
        assert!(
            text.contains("asap_phase_runs_total{phase=\"append\"} 1"),
            "{text}"
        );
        assert!(text.contains(
            "asap_phase_cpu_seconds_total{phase=\"precompute\",group=\"0\",window_ms=\"10000\",views=\"3,5\"} 0"
        ));
        assert!(text.contains("asap_query_latency_seconds_bucket{query=\"sum(x)\",le=\"0.005\"} 1"));
        assert!(text.contains("asap_precompute_backlog_windows 2"));
        assert!(text.contains("asap_thread_cpu_seconds_total{thread="));
    }
}
