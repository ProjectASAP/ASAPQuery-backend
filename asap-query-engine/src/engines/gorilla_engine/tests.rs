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
    ChunkRef, ColdStore, ColdStoreError, PostingsHits, RawSample,
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
    /// **mvp/v5**: optional postings table keyed by `(label_name,
    /// label_value)`. When `Some`, [`ColdStore::list_postings_for`]
    /// answers from this table; when `None`, returns a "missing
    /// postings" outcome (driving the fall-back path test).
    postings: Option<BTreeMap<(String, String), Vec<u64>>>,
}

impl MockColdStore {
    fn new(chunks: Vec<(ChunkRef, Vec<RawSample>)>) -> Self {
        Self {
            chunks,
            read_delay: None,
            postings: None,
        }
    }

    fn with_read_delay(mut self, d: Duration) -> Self {
        self.read_delay = Some(d);
        self
    }

    /// mvp/v5: install a postings table for the
    /// `list_postings_for` path.
    fn with_postings(
        mut self,
        postings: BTreeMap<(String, String), Vec<u64>>,
    ) -> Self {
        self.postings = Some(postings);
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

    async fn list_postings_for(
        &self,
        _metric: &str,
        _start_ms: i64,
        _end_ms: i64,
        matchers: &[(String, String)],
    ) -> Result<PostingsHits, ColdStoreError> {
        let Some(table) = &self.postings else {
            // Mirror "real" missing-postings behaviour: the trait
            // says return Unsupported when the backend doesn't
            // know how to compute this. The executor treats that
            // as fall-through.
            return Err(ColdStoreError::Unsupported("list_postings_for"));
        };
        let mut hits = PostingsHits {
            series_ids: Vec::new(),
            buckets_in_range: 1,
            buckets_with_postings: 1,
        };
        if matchers.is_empty() {
            // Union of every series id in the table.
            let mut set: std::collections::BTreeSet<u64> =
                std::collections::BTreeSet::new();
            for ids in table.values() {
                set.extend(ids.iter().copied());
            }
            hits.series_ids = set.into_iter().collect();
            return Ok(hits);
        }
        let first = table
            .get(&matchers[0])
            .cloned()
            .unwrap_or_default();
        let mut acc: std::collections::BTreeSet<u64> = first.into_iter().collect();
        for m in &matchers[1..] {
            let next = table.get(m).cloned().unwrap_or_default();
            let next_set: std::collections::BTreeSet<u64> = next.into_iter().collect();
            acc = acc.intersection(&next_set).copied().collect();
        }
        hits.series_ids = acc.into_iter().collect();
        Ok(hits)
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
        chunks_skipped_via_postings: 0,
        postings_filtered_series_count: 0,
        postings_missing: false,
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


// ─────────────────────────────────────────────────────────────────────
// mvp/v5 — postings-aware path tests
// ─────────────────────────────────────────────────────────────────────

/// Build a chunk with explicit `label_hash` so the postings-aware
/// path can prune via `series_id == label_hash`.
fn labeled_chunk(
    key: &str,
    label_hash: u64,
    label_value: &str,
    start_ms: i64,
    samples: &[(i64, f64)],
) -> (ChunkRef, Vec<RawSample>) {
    let last_ts = samples.last().map(|(t, _)| *t).unwrap_or(start_ms);
    let chunk = ChunkRef {
        key: key.to_string(),
        metric: METRIC.to_string(),
        time_range_ms: (start_ms, last_ts + 1),
        label_hash,
        sample_count: samples.len() as u32,
        size_bytes: 0,
    };
    let mut labels = BTreeMap::new();
    labels.insert("zone".to_string(), label_value.to_string());
    let raw_samples: Vec<RawSample> = samples
        .iter()
        .map(|(t, v)| RawSample {
            ts_ms: *t,
            labels: labels.clone(),
            value: *v,
        })
        .collect();
    (chunk, raw_samples)
}

#[tokio::test]
async fn postings_aware_path_prunes_chunks() {
    // Two chunks: one for zone=a (label_hash=11), one for zone=b
    // (label_hash=22). Postings says zone=a → [11]. The engine
    // must read only the zone=a chunk.
    let (chunk_a, samples_a) =
        labeled_chunk("k-a", 11, "a", NOW_MS - 30_000, &[(NOW_MS - 1_000, 5.0), (NOW_MS - 500, 5.0)]);
    let (chunk_b, samples_b) =
        labeled_chunk("k-b", 22, "b", NOW_MS - 30_000, &[(NOW_MS - 1_000, 99.0), (NOW_MS - 500, 99.0)]);
    let mut postings: BTreeMap<(String, String), Vec<u64>> = BTreeMap::new();
    postings.insert(("zone".to_string(), "a".to_string()), vec![11]);
    postings.insert(("zone".to_string(), "b".to_string()), vec![22]);
    let mock = MockColdStore::new(vec![(chunk_a, samples_a), (chunk_b, samples_b)])
        .with_postings(postings);
    let engine = GorillaQueryEngine::new(Arc::new(mock), cfg());
    let plan =
        plan_query_at(&format!(r#"sum_over_time({METRIC}{{zone="a"}}[5m])"#), NOW_MS).unwrap();
    assert_eq!(plan.label_matchers.len(), 1);
    let exec = ExactExecutor::new(engine.cold_store_for_tests(), cfg());
    let outcome = exec.execute_plan(&plan).await.unwrap();
    // Only zone=a chunk contributed: 5.0 + 5.0 = 10.0 (NOT 5+5+99+99=208).
    assert_eq!(outcome.value, 10.0);
    assert_eq!(outcome.chunks_fetched, 1);
    assert_eq!(outcome.chunks_skipped_via_postings, 1);
    assert_eq!(outcome.postings_filtered_series_count, 1);
    assert!(!outcome.postings_missing);
}

#[tokio::test]
async fn postings_missing_falls_back_to_scan_all() {
    // Same chunks, NO postings table → the executor falls through
    // to the scan-all path and uses the post-decode label filter
    // for correctness. The `postings_missing` flag must be set.
    let (chunk_a, samples_a) =
        labeled_chunk("k-a", 11, "a", NOW_MS - 30_000, &[(NOW_MS - 1_000, 5.0)]);
    let (chunk_b, samples_b) =
        labeled_chunk("k-b", 22, "b", NOW_MS - 30_000, &[(NOW_MS - 1_000, 99.0)]);
    let mock = MockColdStore::new(vec![(chunk_a, samples_a), (chunk_b, samples_b)]);
    let engine = GorillaQueryEngine::new(Arc::new(mock), cfg());
    let plan =
        plan_query_at(&format!(r#"sum_over_time({METRIC}{{zone="a"}}[5m])"#), NOW_MS).unwrap();
    let exec = ExactExecutor::new(engine.cold_store_for_tests(), cfg());
    let outcome = exec.execute_plan(&plan).await.unwrap();
    // Correctness: only zone=a sample (5.0) folded in. The
    // post-decode filter does the work.
    assert_eq!(outcome.value, 5.0);
    // Both chunks were fetched — postings filter no-oped.
    assert_eq!(outcome.chunks_fetched, 2);
    assert_eq!(outcome.chunks_skipped_via_postings, 0);
    assert!(outcome.postings_missing, "missing-postings flag must be set");
    let infos = outcome.info_lines();
    assert!(infos.iter().any(|i| i == "data_source_quirk: postings_missing"));
}

#[tokio::test]
async fn postings_path_no_label_predicate_skips_postings_lookup() {
    // No label predicate → postings filter is a no-op; the
    // postings table is never consulted. Total = 5+99 = 104.
    let (chunk_a, samples_a) =
        labeled_chunk("k-a", 11, "a", NOW_MS - 30_000, &[(NOW_MS - 1_000, 5.0)]);
    let (chunk_b, samples_b) =
        labeled_chunk("k-b", 22, "b", NOW_MS - 30_000, &[(NOW_MS - 1_000, 99.0)]);
    let mock = MockColdStore::new(vec![(chunk_a, samples_a), (chunk_b, samples_b)]);
    let engine = GorillaQueryEngine::new(Arc::new(mock), cfg());
    let plan = plan_query_at(&format!("sum_over_time({METRIC}[5m])"), NOW_MS).unwrap();
    assert!(plan.label_matchers.is_empty());
    let exec = ExactExecutor::new(engine.cold_store_for_tests(), cfg());
    let outcome = exec.execute_plan(&plan).await.unwrap();
    assert_eq!(outcome.value, 104.0);
    assert_eq!(outcome.chunks_fetched, 2);
    assert!(!outcome.postings_missing);
    assert_eq!(outcome.chunks_skipped_via_postings, 0);
}

