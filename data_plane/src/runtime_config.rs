//! Runtime controls shared by the server and the overhead inspection harness.
use clap::Args;
use serde::Serialize;
use std::{num::NonZeroUsize, path::Path};

#[derive(Args, Clone, Debug, Serialize)]
pub struct RuntimeConfig {
    /// Tokio workers; defaults to TOKIO_WORKER_THREADS, then available parallelism.
    #[arg(long)]
    pub runtime_workers: Option<NonZeroUsize>,
    /// Upper bound on Tokio's separate, lazily created blocking pool.
    #[arg(long, default_value = "512")]
    pub runtime_max_blocking_threads: NonZeroUsize,
}
impl RuntimeConfig {
    pub fn workers(&self) -> anyhow::Result<usize> {
        if let Some(n) = self.runtime_workers {
            return Ok(n.get());
        }
        if let Ok(n) = std::env::var("TOKIO_WORKER_THREADS") {
            return Ok(n.parse::<NonZeroUsize>()?.get());
        }
        Ok(std::thread::available_parallelism()?.get())
    }
    pub fn build(&self) -> anyhow::Result<tokio::runtime::Runtime> {
        Ok(tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.workers()?)
            .max_blocking_threads(self.runtime_max_blocking_threads.get())
            .thread_name("asap-runtime")
            .enable_all()
            .build()?)
    }
}

#[derive(Args, Clone, Debug, Serialize)]
pub struct LogConfig {
    /// RUST_LOG takes precedence over this filter.
    #[arg(long, default_value = "info")]
    pub log_level: String,
    #[arg(long)]
    pub disable_console_log: bool,
    #[arg(long)]
    pub disable_file_log: bool,
}
impl LogConfig {
    pub fn filter(&self) -> String {
        std::env::var("RUST_LOG").unwrap_or_else(|_| self.log_level.clone())
    }
    pub fn init(
        &self,
        directory: &Path,
    ) -> anyhow::Result<Option<tracing_appender::non_blocking::WorkerGuard>> {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
        let filter = EnvFilter::try_new(self.filter())?;
        let console = (!self.disable_console_log).then(|| {
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true)
                .with_target(true)
                .with_writer(std::io::stdout)
        });
        let (file, guard) = if self.disable_file_log {
            (None, None)
        } else {
            std::fs::create_dir_all(directory)?;
            let (writer, guard) = tracing_appender::non_blocking(tracing_appender::rolling::never(
                directory,
                "query_engine.log",
            ));
            (
                Some(
                    tracing_subscriber::fmt::layer()
                        .with_file(true)
                        .with_line_number(true)
                        .with_target(true)
                        .with_ansi(false)
                        .with_writer(writer),
                ),
                Some(guard),
            )
        };
        tracing_subscriber::registry()
            .with(filter)
            .with(console)
            .with(file)
            .try_init()?;
        Ok(guard)
    }
}

/// Unknown OS-specific measurements remain null, never zero.
pub fn process_snapshot() -> serde_json::Value {
    let read = |p: &str| std::fs::read_to_string(p).ok();
    let status = read("/proc/self/status");
    let status_bytes = |name: &str| {
        status.as_deref().and_then(|s| {
            s.lines().find_map(|line| {
                line.strip_prefix(name)?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()?
                    .checked_mul(1024)
            })
        })
    };
    let threads: Vec<_> = std::fs::read_dir("/proc/self/task").into_iter().flatten()
        .filter_map(Result::ok).map(|e| serde_json::json!({"tid":e.file_name().to_string_lossy(), "name":std::fs::read_to_string(e.path().join("comm")).ok()})).collect();
    let cgroup = read("/proc/self/cgroup");
    let relative = cgroup
        .as_deref()
        .and_then(|s| s.lines().find_map(|line| line.strip_prefix("0::")));
    let group = relative.map(|s| Path::new("/sys/fs/cgroup").join(s.trim_start_matches('/')));
    let limits = group.map(|p| {
        let ancestors: Vec<_> = p.ancestors().take_while(|a| a.starts_with("/sys/fs/cgroup"))
            .map(|a| serde_json::json!({
                "path": a,
                "cpu_max": std::fs::read_to_string(a.join("cpu.max")).ok(),
                "cpuset_effective": std::fs::read_to_string(a.join("cpuset.cpus.effective")).ok(),
                "memory_max": std::fs::read_to_string(a.join("memory.max")).ok()
            })).collect();
        serde_json::json!({"path": p, "visible_ancestors_including_self": ancestors})
    });
    serde_json::json!({"cpu_model":read("/proc/cpuinfo").and_then(|s|s.lines().find_map(|l|l.strip_prefix("model name").map(|v|v.trim_start_matches([' ', ':', '\t']).to_owned()))),
        "kernel":read("/proc/sys/kernel/osrelease"), "pid":std::process::id(), "available_parallelism":std::thread::available_parallelism().ok().map(|n|n.get()),
        "rss_bytes":status_bytes("VmRSS:"), "peak_rss_bytes":status_bytes("VmHWM:"), "status":status, "threads":threads, "cgroup_membership":cgroup, "cgroup_v2":limits})
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        runtime: RuntimeConfig,
        #[command(flatten)]
        logs: LogConfig,
    }
    /// Zero-sized pools fail at argument parsing rather than panicking at startup.
    #[test]
    fn rejects_zero_pools() {
        for flag in ["--runtime-workers", "--runtime-max-blocking-threads"] {
            assert!(Cli::try_parse_from(["test", flag, "0"]).is_err());
        }
    }
    /// Explicit worker configuration reaches the real Tokio runtime.
    #[test]
    fn configures_workers() {
        let cli = Cli::parse_from([
            "test",
            "--runtime-workers",
            "2",
            "--runtime-max-blocking-threads",
            "3",
        ]);
        let runtime = cli.runtime.build().unwrap();
        assert_eq!(runtime.metrics().num_workers(), 2);
        assert_eq!(cli.runtime.runtime_max_blocking_threads.get(), 3);
    }
}
