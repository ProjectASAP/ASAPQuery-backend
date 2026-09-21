use thiserror::Error;

/// Controls whether query-serving code may contact an external backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryForwardingPolicy {
    #[default]
    Enabled,
    Disabled,
}

impl QueryForwardingPolicy {
    pub const fn allows_external_queries(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Typed error for an external query blocked by the test policy.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueryForwardingError {
    #[error("external query forwarding is disabled for backend {backend} ({path})")]
    Disabled {
        backend: &'static str,
        path: &'static str,
    },
}
