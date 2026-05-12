use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the SimpleMapStore persistence layer.
///
/// The two bounding knobs, in priority order:
///
/// * **Memory budget (primary).** When in-memory sealed-epoch bytes exceed
///   `memory_limit_bytes`, the background flusher evicts oldest-sealed-epoch
///   first (globally, by `end_ts`) until usage drops below
///   `memory_low_watermark_bytes`. If usage reaches `hard_cap_bytes` inside
///   an insert, the insert path blocks until the flusher catches up.
///
/// * **Time watermark (secondary).** Any sealed epoch whose `end_ts` is
///   older than `now - hot_window_ms` is flushed on the next tick even if
///   the store is nowhere near the memory budget. Guarantees a predictable
///   hot-set size under light ingest.
///
/// Plus a disk-retention knob:
///
/// * **Cold TTL.** Any part whose `max_ts` is older than
///   `now - delete_older_than_ms` is deleted from disk on the next tick.
///   Bounds manifest size and long-running directory growth.
#[derive(Debug, Clone)]
pub struct SimpleMapStorePersistenceConfig {
    // ---- Primary: memory budget ----
    pub memory_limit_bytes: usize,
    pub memory_low_watermark_bytes: usize,
    pub hard_cap_bytes: usize,

    // ---- Secondary: time watermark T ----
    /// Hot-window length in milliseconds. `None` disables time-based flushing.
    pub hot_window_ms: Option<u64>,

    // ---- Disk retention ----
    /// Cold-tier TTL in milliseconds. `None` disables cold deletion.
    /// Should be much larger than `hot_window_ms` (hours or days).
    pub delete_older_than_ms: Option<u64>,

    // ---- Misc ----
    /// Cadence of the background flusher loop.
    pub flush_interval: Duration,

    /// Root directory for segment files and the parts manifest.
    pub disk_path: PathBuf,

    /// Tier-2 part-cache byte budget. 0 disables the cache (every cold
    /// query pays disk I/O). Default is `min(10% * memory_limit_bytes,
    /// 512 MiB)` — scale it with the write budget, not a fixed number.
    pub part_cache_bytes: u64,
}

impl SimpleMapStorePersistenceConfig {
    /// Build a config with sensible defaults relative to a memory budget.
    pub fn with_memory_limit(memory_limit_bytes: usize, disk_path: PathBuf) -> Self {
        let low_water = memory_limit_bytes * 8 / 10; // 80% of high water
        let hard_cap = memory_limit_bytes * 125 / 100; // 125% of high water
        let cache = (memory_limit_bytes / 10).min(512 * 1024 * 1024) as u64;
        Self {
            memory_limit_bytes,
            memory_low_watermark_bytes: low_water,
            hard_cap_bytes: hard_cap,
            hot_window_ms: Some(60 * 60 * 1000), // 1 hour
            delete_older_than_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days
            flush_interval: Duration::from_secs(1),
            disk_path,
            part_cache_bytes: cache,
        }
    }
}
