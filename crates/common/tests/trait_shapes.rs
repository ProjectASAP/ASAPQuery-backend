//! Each boundary trait can be implemented outside this crate and used as
//! `Arc<dyn …>`, as the deployment holds its engines, stores and fallback.
//! The fakes are trivial: only the trait shapes are under test.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common::{
    AnswerSource, EngineQueryResult, FallbackClient, FallbackError, FallbackResponse, IngestEngine,
    IngestError, InstallError, InstalledPlan, PlanId, QueryEngine, QueryEngineError, QueryEval,
    QueryLanguage, QueryRequest, RawDataBatch, RawDataRetention, RawDataStore, Source, ViewId,
    ViewInstance, ViewInstanceKey, ViewInstanceMetadata, ViewState, ViewStore,
};

struct NoIngest;

#[async_trait]
impl IngestEngine for NoIngest {
    fn install(&self, _plan: InstalledPlan) -> Result<(), InstallError> {
        Ok(())
    }
    async fn ingest(&self, _batch: RawDataBatch) -> Result<(), IngestError> {
        Ok(())
    }
    fn shutdown(&self) {}
}

struct NeverReady;

#[async_trait]
impl QueryEngine for NeverReady {
    fn install(&self, _plan: InstalledPlan) -> Result<(), InstallError> {
        Ok(())
    }
    async fn execute_batch(
        &self,
        queries: &[(AnswerSource, QueryRequest)],
    ) -> Vec<Result<EngineQueryResult, QueryEngineError>> {
        queries
            .iter()
            .map(|_| Ok(EngineQueryResult::NotReady))
            .collect()
    }
}

struct NoViews;

impl ViewStore for NoViews {
    fn put(&self, _key: ViewInstanceKey, _instance: ViewInstance) {}
    fn get(&self, _key: &ViewInstanceKey) -> Option<ViewInstance> {
        None
    }
    fn instances(
        &self,
        _view: &ViewId,
        _interval_ms: (i64, i64),
    ) -> Vec<((i64, i64), ViewInstance)> {
        Vec::new()
    }
    fn evict_before(&self, _view: &ViewId, _before_ms: i64) {}
    fn timestamp_range_per_view(&self) -> HashMap<ViewId, (i64, i64)> {
        HashMap::new()
    }
}

struct NoRawData;

impl RawDataStore for NoRawData {
    fn append(&self, _source: &Source, _batch: RawDataBatch) {}
    fn read(&self, _source: &Source, _interval_ms: (i64, i64)) -> Option<RawDataBatch> {
        None
    }
    fn evict(&self, _retention: &[RawDataRetention], _now_ms: i64) {}
}

struct Unreachable;

#[async_trait]
impl FallbackClient for Unreachable {
    async fn execute(
        &self,
        _request: &QueryRequest,
        _headers: &[(String, String)],
    ) -> Result<FallbackResponse, FallbackError> {
        Err(FallbackError::new("connection refused"))
    }
}

#[tokio::test]
async fn every_trait_is_usable_as_a_trait_object() {
    let ingest: Arc<dyn IngestEngine> = Arc::new(NoIngest);
    let query: Arc<dyn QueryEngine> = Arc::new(NeverReady);
    let views: Arc<dyn ViewStore> = Arc::new(NoViews);
    let raw: Arc<dyn RawDataStore> = Arc::new(NoRawData);
    let fallback: Arc<dyn FallbackClient> = Arc::new(Unreachable);

    let labels = [("__name__".to_string(), "up".to_string())].into();
    let batch = RawDataBatch::TimeSeries {
        series: vec![(labels, vec![(1_000, 1.0)])],
    };
    assert_eq!(ingest.ingest(batch).await, Ok(()));
    ingest.shutdown();

    let request = QueryRequest {
        language: QueryLanguage::PromQL,
        query: "up".into(),
        eval: QueryEval::Range {
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 15_000,
        },
    };
    let source = AnswerSource::Planned {
        plan: PlanId(1),
        root: 0,
    };
    assert_eq!(
        query.execute(&source, &request).await,
        Ok(EngineQueryResult::NotReady)
    );

    let view = ViewId {
        plan: PlanId(1),
        node: 3,
    };
    let key = ViewInstanceKey {
        view,
        window_ms: (0, 10_000),
    };
    views.put(
        key,
        ViewInstance {
            state: ViewState(Arc::new(0_u64)),
            metadata: ViewInstanceMetadata { size_bytes: 8 },
        },
    );
    assert!(views.get(&key).is_none());

    let metric = Source::TimeSeries {
        metric: "up".into(),
    };
    raw.evict(&[], 0);
    assert_eq!(raw.read(&metric, (0, 1)), None);

    assert!(fallback.execute(&request, &[]).await.is_err());
}
