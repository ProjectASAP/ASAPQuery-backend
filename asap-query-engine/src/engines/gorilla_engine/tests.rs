//! Phase-4 unit tests for the Gorilla query engine.
//!
//! Tests exercise the engine end-to-end via a `MockColdStore`
//! injected in place of the production `GorillaS3ColdStore`. The
//! mock is intentionally minimal: it owns a `Vec<(ChunkRef,
//! Vec<RawSample>)>` and answers `list_chunks` / `read_chunk`
//! straight off it, with optional latency injection for the
//! timeout test.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;

use crate::drivers::query::fallback::cold_store::{
    ChunkRef, ColdStore, ColdStoreError, RawSample,
};
use crate::engines::query_result::QueryResult;
use crate::stores::sketch_db::accuracy::{AccuracyKind, AccuracyProfile};

use super::query_planner::{plan_query_at, QueryStatistic};
use super::{
    wrap_result, EngineError, ExactExecutor, ExecutionOutcome, GorillaEngineConfig,
    GorillaQueryEngine, DATA_SOURCE_GORILLA_ARCHIVE,
};

// ─────────────────────────────────────────────────────────────────────
// Mock cold store
// ─────────────────────────────────────────────────────────────────────

/// In-process mock that satisfies the [`ColdStore`] trait without
/// any S3 / disk roundtrip. Built once in each test from a list of
/// `(ChunkRef, samples)` pairs.
#[derive(Default)]
struct MockColdStore {
    chunks: Vec<(ChunkRef, Vec<RawSample>)>,
    /// If set, every `read_chunk` call sleeps for this duration —
    /// used by the timeout test.
    read_delay: Option<Duration>,
}

impl MockColdStore {
    fn new(chunks: Vec<(ChunkRef, Vec<RawSample>)>) -> Self {
        Self {
            chunks,
            read_delay: None,
        }
    }

    fn with_read_delay(mut self, d: Duration) -> Self {
        self.read_delay = Some(d);
        self
    }
}

#[async_trait]
impl ColdStore for MockColdStore {
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, ColdStoreError> {
        let mut out = Vec::new();
        let chunks = self.list_chunks(metric, start_ms, end_ms).await?;
        for c in chunks {
            for s in self.read_chunk(&c).await? {
                if s.ts_ms >= start_ms && s.ts_ms < end_ms {
                    out.push(s);
                }
            }
        }
        Ok(out)
    }

    async fn list_chunks(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ChunkRef>, ColdStoreError> {
        Ok(self
            .chunks
            .iter()
            .filter(|(c, _)| {
                c.metric == metric
                    && c.time_range_ms.0 < end_ms
                    && c.time_range_ms.1 >= start_ms
            })
            .map(|(c, _)| c.clone())
            .collect())
    }

    async fn read_chunk(&self, chunk: &ChunkRef) -> Result<Vec<RawSample>, ColdStoreError> {
        if let Some(d) = self.read_delay {
            sleep(d).await;
        }
        for (c, samples) in &self.chunks {
            if c.key == chunk.key {
                return Ok(samples.clone());
            }
        }
        Err(ColdStoreError::Backend(format!(
            "mock: no such chunk {}",
            chunk.key
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────
// Fixture helpers
// ─────────────────────────────────────────────────────────────────────

const NOW_MS: i64 = 1_715_000_000_000;
const METRIC: &str = "http_requests_total";

fn raw(ts_ms: i64, value: f64) -> RawSample {
    RawSample {
        ts_ms,
        labels: BTreeMap::new(),
        value,
    }
}

/// One chunk covering `[start, start + n*step]` with a
/// monotonically-increasing value column (`base + i*step_v`).
fn linear_chunk(
    key: &str,
    start_ms: i64,
    step_ms: i64,
    n: usize,
    base: f64,
    step_v: f64,
) -> (ChunkRef, Vec<RawSample>) {
    let samples: Vec<RawSample> = (0..n)
        .map(|i| raw(start_ms + (i as i64) * step_ms, base + (i as f64) * step_v))
        .collect();
    let last_ts = samples.last().map(|s| s.ts_ms).unwrap_or(start_ms);
    let chunk = ChunkRef {
        key: key.to_string(),
        metric: METRIC.to_string(),
        time_range_ms: (start_ms, last_ts + 1),
        label_hash: 0,
        sample_count: n as u32,
        size_bytes: 0,
    };
    (chunk, samples)
}

fn cfg() -> GorillaEngineConfig {
    GorillaEngineConfig {
        max_buffered_samples: 1_000_000,
        query_timeout_secs: 30,
    }
}

fn engine_with(chunks: Vec<(ChunkRef, Vec<RawSample>)>) -> GorillaQueryEngine {
    GorillaQueryEngine::new(Arc::new(MockColdStore::new(chunks)), cfg())
}

fn engine_with_config(
    chunks: Vec<(ChunkRef, Vec<RawSample>)>,
    config: GorillaEngineConfig,
) -> GorillaQueryEngine {
    GorillaQueryEngine::new(Arc::new(MockColdStore::new(chunks)), config)
}

// ─────────────────────────────────────────────────────────────────────
// Streaming-additive happy paths
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn execute_sum_over_time_streaming() {
    // 60 samples × value 2.0 = 120.0
    let chunks = vec![linear_chunk(
        "c1",
        NOW_MS - 60_000,
        1_000,
        60,
        2.0,
        0.0,
    )];
    let engine = engine_with(chunks);
    let plan = plan_query_at(&format!("sum_over_time({METRIC}[5m])"), NOW_MS).unwrap();
    assert_eq!(plan.statistic, QueryStatistic::SumOverTime);

    let exec = ExactExecutor::new(
        Arc::new(MockColdStore::new(vec![linear_chunk(
            "c1",
            NOW_MS - 60_000,
            1_000,
            60,
            2.0,
            0.0,
        )])),
        cfg(),
    );
    let outcome = exec.execute_plan(&plan).await.unwrap();
    assert_eq!(outcome.value, 120.0);
    assert_eq!(outcome.samples_scanned, 60);
    assert_eq!(outcome.chunks_fetched, 1);

    // Also verify via the high-level engine.
    let result = engine
        .execute_at(&format!("sum_over_time({METRIC}[5m])"), NOW_MS)
        .await
        .unwrap();
    assert!(matches!(result, QueryResult::Vector(_)));
}

#[tokio::test]
async fn execute_count_over_time() {
    let chunks = vec![linear_chunk("c1", NOW_MS - 30_000, 1_000, 30, 0.0, 0.0)];
    let engine = engine_with(chunks);
    let plan = plan_query_at(&format!("count_over_time({METRIC}[1m])"), NOW_MS).unwrap();
    let exec = ExactExecutor::new(
        Arc::new(MockColdStore::new(vec![linear_chunk(
            "c1",
            NOW_MS - 30_000,
            1_000,
            30,
            0.0,
            0.0,
        )])),
        cfg(),
    );
    let outcome = exec.execute_plan(&plan).await.unwrap();
    assert_eq!(outcome.value, 30.0);

    let result = engine
        .execute_at(&format!("count_over_time({METRIC}[1m])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert_eq!(iv.values[0].value, 30.0);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_avg_over_time() {
    // 4 samples: 1, 2, 3, 4 → avg = 2.5
    let chunks = vec![linear_chunk("c1", NOW_MS - 4_000, 1_000, 4, 1.0, 1.0)];
    let engine = engine_with(chunks);
    let result = engine
        .execute_at(&format!("avg_over_time({METRIC}[10s])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert!((iv.values[0].value - 2.5).abs() < 1e-12);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_min_over_time() {
    // values 5, 1, 3, 4 → min = 1
    let samples = vec![
        raw(NOW_MS - 4_000, 5.0),
        raw(NOW_MS - 3_000, 1.0),
        raw(NOW_MS - 2_000, 3.0),
        raw(NOW_MS - 1_000, 4.0),
    ];
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 4_000, NOW_MS),
        label_hash: 0,
        sample_count: 4,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("min_over_time({METRIC}[10s])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert_eq!(iv.values[0].value, 1.0);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_max_over_time() {
    let samples = vec![
        raw(NOW_MS - 4_000, 5.0),
        raw(NOW_MS - 3_000, 1.0),
        raw(NOW_MS - 2_000, 3.0),
        raw(NOW_MS - 1_000, 4.0),
    ];
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 4_000, NOW_MS),
        label_hash: 0,
        sample_count: 4,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("max_over_time({METRIC}[10s])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert_eq!(iv.values[0].value, 5.0);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_rate_basic() {
    // Counter goes from 100 at t=NOW-10s to 200 at t=NOW-1s.
    // rate over 10s window = (200 - 100) / 10s = 10.0
    let samples = vec![
        raw(NOW_MS - 10_000, 100.0),
        raw(NOW_MS - 5_000, 150.0),
        raw(NOW_MS - 1_000, 200.0),
    ];
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 10_000, NOW_MS),
        label_hash: 0,
        sample_count: 3,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("rate({METRIC}[10s])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert!((iv.values[0].value - 10.0).abs() < 1e-9);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_increase_basic() {
    let samples = vec![
        raw(NOW_MS - 10_000, 100.0),
        raw(NOW_MS - 5_000, 150.0),
        raw(NOW_MS - 1_000, 250.0),
    ];
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 10_000, NOW_MS),
        label_hash: 0,
        sample_count: 3,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("increase({METRIC}[10s])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert!((iv.values[0].value - 150.0).abs() < 1e-9);
    } else {
        panic!("expected Vector");
    }
}

// ─────────────────────────────────────────────────────────────────────
// Buffered quantile + topk
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn execute_quantile_buffered_basic() {
    // Values 0..100; q0.99 → index round((100-1)*0.99) = round(98.01) = 98 → value 98.
    let mut samples: Vec<RawSample> = (0..100)
        .map(|i| raw(NOW_MS - 100_000 + (i as i64) * 1_000, i as f64))
        .collect();
    // Shuffle the value order so the executor must sort.
    samples.sort_by_key(|s| s.value as i64);
    samples.reverse();

    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 100_000, NOW_MS),
        label_hash: 0,
        sample_count: 100,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("quantile_over_time(0.99, {METRIC}[2m])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert_eq!(iv.values[0].value, 98.0);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_quantile_too_many_samples_errors() {
    // Generate 100 samples but cap the buffered budget at 5.
    let samples: Vec<RawSample> = (0..100)
        .map(|i| raw(NOW_MS - 100_000 + (i as i64) * 1_000, i as f64))
        .collect();
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 100_000, NOW_MS),
        label_hash: 0,
        sample_count: 100,
        size_bytes: 0,
    };
    let cfg = GorillaEngineConfig {
        max_buffered_samples: 5,
        query_timeout_secs: 30,
    };
    let engine = engine_with_config(vec![(chunk, samples)], cfg);
    let res = engine
        .execute_at(&format!("quantile_over_time(0.5, {METRIC}[2m])"), NOW_MS)
        .await;
    match res {
        Err(EngineError::TooManySamples { count, limit }) => {
            assert_eq!(limit, 5);
            assert!(count > limit);
        }
        other => panic!("expected TooManySamples, got {other:?}"),
    }
}

#[tokio::test]
async fn execute_topk_basic() {
    // Values [1, 2, 3, 10, 20]; topk(2) → 30
    let samples = vec![
        raw(NOW_MS - 5_000, 1.0),
        raw(NOW_MS - 4_000, 2.0),
        raw(NOW_MS - 3_000, 3.0),
        raw(NOW_MS - 2_000, 10.0),
        raw(NOW_MS - 1_000, 20.0),
    ];
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 5_000, NOW_MS),
        label_hash: 0,
        sample_count: 5,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("topk(2, sum_over_time({METRIC}[10s]))"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert_eq!(iv.values[0].value, 30.0);
    } else {
        panic!("expected Vector");
    }
}

// ─────────────────────────────────────────────────────────────────────
// Edge cases
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn execute_empty_chunks_returns_zero_or_nan() {
    let engine = engine_with(Vec::new());
    let sum = engine
        .execute_at(&format!("sum_over_time({METRIC}[5m])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = sum {
        assert!(iv.values[0].value.is_nan(), "sum on empty should be NaN");
    } else {
        panic!("expected Vector");
    }
    let count = engine
        .execute_at(&format!("count_over_time({METRIC}[5m])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = count {
        assert_eq!(iv.values[0].value, 0.0);
    } else {
        panic!("expected Vector");
    }
}

#[tokio::test]
async fn execute_chunks_partially_outside_range_filtered() {
    // Chunk has 100 samples spanning [NOW-100s, NOW]; request
    // covers the latter half [NOW-50s, NOW] → exactly 50 samples
    // contribute.
    let samples: Vec<RawSample> = (0..100)
        .map(|i| raw(NOW_MS - 100_000 + (i as i64) * 1_000, 1.0))
        .collect();
    let chunk = ChunkRef {
        key: "c".into(),
        metric: METRIC.into(),
        time_range_ms: (NOW_MS - 100_000, NOW_MS),
        label_hash: 0,
        sample_count: 100,
        size_bytes: 0,
    };
    let engine = engine_with(vec![(chunk, samples)]);
    let result = engine
        .execute_at(&format!("count_over_time({METRIC}[50s])"), NOW_MS)
        .await
        .unwrap();
    if let QueryResult::Vector(iv) = result {
        assert_eq!(
            iv.values[0].value, 50.0,
            "exactly 50 samples should match a 50s window"
        );
    } else {
        panic!("expected Vector");
    }
}

// ─────────────────────────────────────────────────────────────────────
// Result wrapping
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn result_carries_exact_accuracy_envelope() {
    let chunks = vec![linear_chunk("c", NOW_MS - 1_000, 100, 10, 1.0, 0.0)];
    let engine = engine_with(chunks);
    let result = engine
        .execute_at(&format!("sum_over_time({METRIC}[5s])"), NOW_MS)
        .await
        .unwrap();
    let env = result
        .accuracy()
        .expect("Gorilla engine result must carry an accuracy envelope");
    assert_eq!(env.profile.kind, AccuracyKind::Exact);
    assert_eq!(env.profile.epsilon, 0.0);
    assert_eq!(env.profile.delta, 0.0);
    // And the summary string the dashboards parse:
    assert_eq!(env.profile.summary(), AccuracyProfile::exact().summary());
}

#[tokio::test]
async fn result_includes_data_source_gorilla_archive() {
    // The wrapping fn surfaces the data_source line on
    // ExecutionOutcome::info_lines — pin both the marker constant
    // and the assembled info strings.
    let outcome = ExecutionOutcome {
        value: 42.0,
        samples_scanned: 7,
        chunks_fetched: 2,
    };
    let infos = outcome.info_lines();
    assert!(
        infos.contains(&DATA_SOURCE_GORILLA_ARCHIVE.to_string()),
        "infos must include `{DATA_SOURCE_GORILLA_ARCHIVE}`; got {infos:?}"
    );
    assert!(
        infos.iter().any(|i| i == "samples_scanned: 7"),
        "infos must report the scanned-samples count"
    );
    assert!(
        infos.iter().any(|i| i == "chunks_fetched: 2"),
        "infos must report the chunk-fetch count"
    );

    // And via the wrap_result path, the QueryResult itself carries
    // the exact-accuracy envelope (data_source line is on the
    // info-array which is assembled at the HTTP-driver layer; see
    // wrap_result docs).
    let plan = plan_query_at(&format!("sum_over_time({METRIC}[5s])"), NOW_MS).unwrap();
    let qr = wrap_result(&plan, outcome.clone());
    let env = qr.accuracy().expect("wrap_result must attach envelope");
    assert_eq!(env.profile.kind, AccuracyKind::Exact);
}

// ─────────────────────────────────────────────────────────────────────
// Timeout
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn engine_respects_config_timeout() {
    // 1 chunk + 250 ms read delay; engine timeout = 1 s ceil. We
    // configure the timeout to 1s (the floor) and force the chunk
    // count up so the cumulative read time > 1 s.
    let mut chunks = Vec::new();
    for i in 0..10 {
        let chunk = ChunkRef {
            key: format!("k-{i}"),
            metric: METRIC.into(),
            time_range_ms: (NOW_MS - 60_000, NOW_MS),
            label_hash: 0,
            sample_count: 1,
            size_bytes: 0,
        };
        chunks.push((chunk, vec![raw(NOW_MS - 1_000, 1.0)]));
    }
    let mock = MockColdStore::new(chunks).with_read_delay(Duration::from_millis(250));
    let cfg = GorillaEngineConfig {
        max_buffered_samples: 1_000_000,
        query_timeout_secs: 1,
    };
    let engine = GorillaQueryEngine::new(Arc::new(mock), cfg);
    let res = engine
        .execute_at(&format!("sum_over_time({METRIC}[5m])"), NOW_MS)
        .await;
    match res {
        Err(EngineError::Timeout(_)) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
}
