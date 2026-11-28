//! `FallbackClient` can be implemented outside this crate and used as
//! `Arc<dyn FallbackClient>`, as the query routes hold their fallback. The
//! fake is trivial: only the trait's shape is under test.

use std::sync::Arc;

use async_trait::async_trait;
use common::{
    FallbackClient, FallbackError, FallbackResponse, QueryEval, QueryLanguage, QueryRequest,
};

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
async fn the_fallback_client_is_usable_as_a_trait_object() {
    let fallback: Arc<dyn FallbackClient> = Arc::new(Unreachable);
    let request = QueryRequest {
        language: QueryLanguage::PromQL,
        query: "up".into(),
        eval: QueryEval::Range {
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 15_000,
        },
    };
    assert!(fallback.execute(&request, &[]).await.is_err());
}
