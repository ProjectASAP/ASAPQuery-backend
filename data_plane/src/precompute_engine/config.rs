use serde::{Deserialize, Serialize};

/// Policy for handling late samples that arrive after their window has closed.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
pub enum LateDataPolicy {
    /// Drop late samples that arrive after their window has closed.
    Drop,
    /// Forward late samples to the store to be merged with existing window data.
    ForwardToStore,
}

/// Configuration for the precompute engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputeEngineConfig {
    /// Number of worker threads for parallel processing.
    pub num_workers: usize,
    /// Maximum allowed lateness for out-of-order samples (milliseconds).
    /// Samples arriving later than this behind the watermark are dropped.
    pub allowed_lateness_ms: i64,
    /// Maximum number of buffered samples per series before oldest are evicted.
    pub max_buffer_per_series: usize,
    /// Interval at which the flush timer fires to close idle windows (milliseconds).
    pub flush_interval_ms: u64,
    /// Capacity of the MPSC channel between router and each worker.
    pub channel_buffer_size: usize,
    /// When true, skip all aggregation and pass each raw sample directly to the
    /// output sink as a `SumAccumulator::with_sum(value)`.
    pub pass_raw_samples: bool,
    /// Aggregation ID to stamp on each raw-mode output.
    pub raw_mode_aggregation_id: u64,
    /// Policy for handling late samples that arrive after their window has closed.
    pub late_data_policy: LateDataPolicy,
    /// Additional idle grace after one window duration. A pane closes when it
    /// has received no input for `window_size + idle_grace`. The legacy YAML
    /// name is accepted as a serde alias. Non-positive disables idle closure.
    #[serde(
        default = "default_wall_clock_idle_grace_period_ms",
        alias = "wall_clock_grace_period_ms"
    )]
    pub wall_clock_idle_grace_period_ms: i64,
    /// Additional grace for the absolute wall-clock deadline. A pane closes
    /// after `window_size + max_open_grace` from its first input even if it is
    /// still active. Non-positive disables the deadline. It stays disabled by
    /// default until late corrections are guaranteed not to be dropped.
    #[serde(default)]
    pub wall_clock_max_open_grace_period_ms: i64,
    /// Optional path where the `SchemaRegistry` persists per-`agg_id`
    /// lifecycle state across restarts (sketch DB Phase 2c). When
    /// set, the registry loads prior `created_at_ms` / `retired_at_ms`
    /// timestamps from the file on startup and atomically rewrites
    /// the file after every config-driven reconcile. When `None`,
    /// the registry is memory-only and the §7 timeline reflects only
    /// post-restart history.
    #[serde(default)]
    pub schema_persist_path: Option<std::path::PathBuf>,
}

impl Default for PrecomputeEngineConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            allowed_lateness_ms: 5_000,
            max_buffer_per_series: 10_000,
            flush_interval_ms: 1_000,
            channel_buffer_size: 10_000,
            pass_raw_samples: false,
            raw_mode_aggregation_id: 0,
            late_data_policy: LateDataPolicy::Drop,
            wall_clock_idle_grace_period_ms: default_wall_clock_idle_grace_period_ms(),
            wall_clock_max_open_grace_period_ms: 0,
            schema_persist_path: None,
        }
    }
}

fn default_wall_clock_idle_grace_period_ms() -> i64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PrecomputeEngineConfig::default();
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.allowed_lateness_ms, 5_000);
        assert_eq!(config.max_buffer_per_series, 10_000);
        assert_eq!(config.flush_interval_ms, 1_000);
        assert_eq!(config.channel_buffer_size, 10_000);
        assert!(!config.pass_raw_samples);
        assert_eq!(config.raw_mode_aggregation_id, 0);
        assert_eq!(config.late_data_policy, LateDataPolicy::Drop);
        assert_eq!(config.wall_clock_idle_grace_period_ms, 5_000);
        assert_eq!(config.wall_clock_max_open_grace_period_ms, 0);
    }

    #[test]
    fn legacy_wall_clock_grace_deserializes_as_idle_grace() {
        let config: PrecomputeEngineConfig = serde_yaml::from_str(
            r#"
num_workers: 1
allowed_lateness_ms: 100
max_buffer_per_series: 10
flush_interval_ms: 1000
channel_buffer_size: 10
pass_raw_samples: false
raw_mode_aggregation_id: 0
late_data_policy: Drop
wall_clock_grace_period_ms: 7000
"#,
        )
        .expect("legacy config should deserialize");
        assert_eq!(config.wall_clock_idle_grace_period_ms, 7_000);
        assert_eq!(config.wall_clock_max_open_grace_period_ms, 0);
    }
}
