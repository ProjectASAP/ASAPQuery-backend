//! `GET /metrics`: the engine's measurements ([`internal_metrics::Metrics`])
//! and the gauges it reads from the stores, the ingestion counters and the
//! result cache, in the Prometheus text format.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use internal_metrics::{thread_cpu_seconds, Metrics, QueryMetrics, WindowLatency};
use storage::store::{QueryTimeResults, ViewTier};

use crate::ingestion::IngestionStats;
use crate::query::Unanswered;
use crate::result_cache::ResultCache;

/// What `/metrics` reports beside [`Metrics`].
pub(crate) struct Gauges<'a> {
    pub stats: IngestionStats,
    pub samples_queued: u64,
    pub precompute_backlog: u64,
    pub persist_backlog: u64,
    pub persist_backlog_bytes: u64,
    pub now_ms: i64,
    pub query_time_results: &'a QueryTimeResults<Unanswered>,
    /// `None` when the result cache is off.
    pub result_cache: Option<&'a ResultCache>,
    /// The views' cold tier, when they are persisted.
    pub view_tier: Option<&'a ViewTier>,
}

/// `metrics` and `gauges` in the Prometheus text format.
pub(crate) fn render(metrics: &Metrics, gauges: &Gauges) -> String {
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
    if let Some(tier) = gauges.view_tier {
        tier.decode.render(&mut out, "phase=\"view_decode\"");
    }
    for (labels, stats) in &metrics.precompute {
        stats.render(&mut out, labels);
    }
    if let Some(push) = &metrics.push {
        push.render(&mut out);
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
        ("late_for_views", stats.samples_late_for_views),
    ] {
        let _ = writeln!(out, "asap_samples_total{{outcome=\"{outcome}\"}} {count}");
    }
    if let Some(push) = &metrics.push {
        push.render_gauges(&mut out);
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
        "Samples queued for the ingest shards.",
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
    if let Some(tier) = gauges.view_tier {
        let cache = tier.cache.stats();
        for (name, help, value) in [
            (
                "asap_view_cache_bytes",
                "Bytes of decoded view windows in the window cache.",
                cache.bytes as f64,
            ),
            (
                "asap_view_cache_entries",
                "View windows in the window cache.",
                cache.entries as f64,
            ),
            (
                "asap_view_pending_bytes",
                "Bytes of view windows committed and not yet written to the view files.",
                load(&tier.pending_bytes),
            ),
            (
                "asap_view_pinned_bytes",
                "Bytes of view windows that cannot be written, kept in memory.",
                load(&tier.pinned_bytes),
            ),
            (
                "asap_view_file_bytes",
                "Bytes of the view files (segments) held.",
                load(&metrics.view_file_bytes),
            ),
        ] {
            gauge(&mut out, name, help, value);
        }
        for (name, help, value) in [
            (
                "asap_view_cache_hits_total",
                "View window reads the window cache answered.",
                cache.hits,
            ),
            (
                "asap_view_cache_misses_total",
                "View window reads the window cache did not answer.",
                cache.misses,
            ),
            (
                "asap_view_cache_evictions_total",
                "View windows evicted from the window cache.",
                cache.evictions,
            ),
            (
                "asap_view_cold_reads_total",
                "View windows decoded from the view files.",
                load(&tier.cold_reads) as u64,
            ),
            (
                "asap_view_cold_read_bytes_total",
                "Bytes of the view files decoded.",
                load(&tier.cold_read_bytes) as u64,
            ),
        ] {
            family(&mut out, name, "counter", help);
            let _ = writeln!(out, "{name} {value}");
        }
    }
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
            "asap_view_segments_marked_cold_total",
            "View segments whose pages were advised cold (MADV_COLD, POSIX_FADV_DONTNEED) once a newer sealed segment of their refresh group existed.",
            load(&metrics.view_segments_marked_cold),
        ),
        (
            "asap_view_persist_over_bound_total",
            "Windows queued for the view files while the queue held more than --max-persist-backlog-bytes (the data plane does not wait for its disk).",
            load(&metrics.view_persist_over_bound),
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

#[cfg(test)]
mod tests {
    use internal_metrics::Timer;

    use super::*;

    /// The text format names each phase and query.
    #[test]
    fn renders_phases_and_queries() {
        let metrics = Metrics::new(
            &[(10_000, vec!["3".into(), "5".into()])],
            &["sum(x)".to_string()],
        );
        metrics.append.record(Timer::start());
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
                view_tier: None,
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
