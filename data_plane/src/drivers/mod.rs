pub mod controller_client;
pub mod ingest;
pub mod query;

// Re-export commonly used types for convenience
pub use controller_client::{spawn_capability_miss_notify, ControllerClient, HttpControllerClient};
pub use ingest::{OtlpReceiver, OtlpReceiverConfig};
pub use query::{AdapterConfig, HttpServer, HttpServerConfig};
