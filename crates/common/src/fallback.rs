use async_trait::async_trait;

use crate::QueryRequest;

/// The client of the backend that answers a query the data plane does not:
/// a query that is not planned, not ready, or failed in the engine.
#[async_trait]
pub trait FallbackClient: Send + Sync {
    /// Sends the query to the backend, with the client's `headers` (name,
    /// value), and returns the backend's response as is.
    async fn execute(
        &self,
        request: &QueryRequest,
        headers: &[(String, String)],
    ) -> Result<FallbackResponse, FallbackError>;
}

/// The backend's HTTP response, passed to the client unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackResponse {
    pub status_code: u16,
    /// The `Content-Type` header value, e.g. `application/json`.
    pub content_type: String,
    pub body: Vec<u8>,
}

/// The backend could not be reached or sent no response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct FallbackError {
    pub message: String,
}

impl FallbackError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
