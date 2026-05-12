//! Minimal end-to-end test for `SimpleMapStorePerKey::with_persistence`.
//!
//! The deeper flush-and-readback tests previously exercised the
//! datafusion-backed `accumulator_serde` path that PR #123 removed.
//! Rather than rebuild that SerDe (the long-term plan is to migrate
//! persistence onto SketchIndex, not back onto SimpleMapStore), those
//! tests were retired in this commit. The single remaining test
//! verifies the construct/drop lifecycle of the flusher thread — it
//! does not touch the disk path.

use std::sync::Arc;

use promql_utilities::data_model::KeyByLabelNames;
use tempfile::TempDir;
use std::time::Duration;

use crate::data_model::{AggregationType, CleanupPolicy, StreamingConfig, WindowType};
use crate::stores::sketch_db::simple_map_store::per_key::SimpleMapStorePerKey;
use crate::stores::sketch_db::simple_map_store::persistence::SimpleMapStorePersistenceConfig;
use crate::AggregationConfig;

fn make_streaming_config(agg_id: u64) -> Arc<StreamingConfig> {
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
        Some(2),
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
